package merkle

import (
	"sort"
	"strings"
	"treeverse-lake/db"
	"treeverse-lake/ident"
	"treeverse-lake/index/model"

	"golang.org/x/xerrors"
)

type CachedNodeUpdater struct {
	db          BTreeReader
	cache       map[string]*model.Node
	dirtyWrites map[string]*model.Node
}

func (c *CachedNodeUpdater) GetNode(addr string) (*model.Node, error) {
	node, exists := c.cache[addr]
	if !exists {
		node, err := c.db.GetNode(addr)
		if xerrors.Is(err, db.ErrNotFound) {
			c.cache[addr] = nil // cache the fact that it doesn't exist
			return nil, db.ErrNotFound
		} else if err != nil {
			return nil, err
		}
		c.cache[addr] = node
	}
	if node != nil {
		return node, nil
	}
	return nil, db.ErrNotFound
}

func (c *CachedNodeUpdater) WriteNode(addr string, node *model.Node) {
	c.cache[addr] = node
	c.dirtyWrites[addr] = node
}

func (c *CachedNodeUpdater) InvalidateNode(addr string) {
	if _, exists := c.dirtyWrites[addr]; exists {
		delete(c.dirtyWrites, addr)
	}
}

func (c *CachedNodeUpdater) Flush(w BTreeWriter) error {
	for addr, node := range c.dirtyWrites {
		err := w.WriteNode(addr, node)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewCachedNodeUpdater(db BTreeReader) *CachedNodeUpdater {
	return &CachedNodeUpdater{
		db:          db,
		cache:       make(map[string]*model.Node),
		dirtyWrites: make(map[string]*model.Node),
	}
}

type BTreeReader interface {
	GetNode(addr string) (*model.Node, error)
}

type BTreeWriter interface {
	WriteNode(addr string, node *model.Node) error
}

type BTree struct {
	root string
	db   BTreeReader
}

type ScanResults struct {
	Entries []*model.Entry
	HasNext bool
}

type TreeChange struct {
	Tombstone bool // if true, this means this path should be deleted from the tree
	Path      string
	Entry     *model.Entry
}

func (t *BTree) Root() string {
	return t.root
}

func (t *BTree) GetEntry(path string) (*model.Entry, error) {
	// start at root
	nodeAddr := t.root
	for {
		// get the current node
		node, err := t.db.GetNode(nodeAddr)
		if err != nil {
			return nil, err
		}
		nodeAddresses := node.GetPointers()
		entries := node.GetEntries()
		for ind, entry := range entries {
			cmp := strings.Compare(path, entry.GetName()) // assuming we are case sensitive
			if cmp == 0 {
				// found an exact match
				return entry, nil
			} else if cmp == 1 {
				// requested path is smaller than the current entry, go to pointers[ind]
				if len(nodeAddresses) < ind+1 {
					// pointing nowhere, so no match in the tree
					return nil, db.ErrNotFound
				}
				nodeAddr = nodeAddresses[ind]
				break // we're done with this node
			} else {
				// requested path is bigger than the current entry, continue checking next entry
				continue
			}
		}
		// requested path is bigger than all entries, go to the right-most node, if it exists
		if len(nodeAddresses) <= len(entries) {
			// no next node to visit
			return nil, db.ErrNotFound
		}
		nodeAddr = nodeAddresses[len(entries)]
	}
}

func (t *BTree) scan(nodeAddr, prefix, from string, amount int, results *ScanResults) error {
	// algorithm:
	// find the smallest value starting with prefix - this is also the first one I see!
	// if "from" is not empty, we start from there instead.
	// from here, go to bigger and bigger nodes until you reach one that doesn't have prefix (or no more nodes)
	// TODO: implement scanning from "from", if it's not empty, for pagination
	node, err := t.db.GetNode(nodeAddr)
	if err != nil {
		return err
	}

	entries := node.GetEntries()
	pointers := node.GetPointers()

	for ind, entry := range entries {
		cmp := strings.Compare(entry.GetName(), prefix)
		if cmp == -1 {
			continue // cannot be smaller than prefix
		}

		if strings.HasPrefix(entry.GetName(), prefix) {
			// this is our first match! - from here we need to walk until we have no more.
			results.Entries = append(results.Entries, entry)
			if len(results.Entries) >= amount {
				return nil // we're done for now
			}
			// go left
			if len(pointers) < ind+2 {
				// no more left pointers, continue to next entry
				continue
			}
			// recurse to the left
			err = t.scan(pointers[ind+1], prefix, from, amount, results)
			if err != nil {
				return err
			}
		} else {
			// no more entries starting with prefix
			results.HasNext = false
			return nil
		}
	}
	// no more entries left
	results.HasNext = false
	return nil
}

func (t *BTree) PrefixScan(prefix, from string, amount int) (*ScanResults, error) {
	// start at root
	results := &ScanResults{
		Entries: make([]*model.Entry, 0),
		HasNext: true,
	}
	err := t.scan(t.root, prefix, from, amount, results)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (t *BTree) Update(w BTreeWriter, branchingFactor int, changes []*TreeChange) (*BTree, error) {
	root := t.root
	nodes := make([]*model.Node, 0)
	// algorithm
	// - for every change we do:
	// - - traverse the tree to find the node that should contain the change
	// - - make the change in the given node
	// - - go to the parent - reflect the changes that need to happen - at a minimum - a pointer has changed
	// - - this ^ happens recursively, until we get to the root node. This means we're creating a new root node for every modification
	// so to recap:
	// - we make 1 change at a time, propagating changes up the tree and replacing the root node every time
	// - once we've reflected all changes, and have the logical nodes built, we write the nodes to DB
	// - at the end, return the last root generated and written

	// use a cached reader to avoid getting the same nodes all the time
	r := NewCachedNodeUpdater(t.db)

	// sort changes by path to ensure minimal retrievals and updates
	sort.SliceStable(changes, func(i, j int) bool {
		return strings.Compare(changes[i].Path, changes[j].Path) == -1
	})

	// calculate the new nodes
	for _, change := range changes {
		// find the node that should contain this change
		currRoot := t.root
		visited := make([]*model.Node, 0)
		for {
			node, err := r.GetNode(currRoot)
			if err != nil {
				return nil, err
			}
			visited = append(visited, node)
			pointers := node.GetPointers()
			entries := node.GetEntries()
			for ind, entry := range entries {
				cmp := strings.Compare(change.Path, entry.GetName())

				if cmp == 0 {
					// this is the node - we need to replace whatever value we had with the current change
					// and propagate this up the tree resulting in a new root

					// TODO: this flow is for an update - we need to also implement a deletion!!!
					// TODO: btw, deletions only happen here, i.e. if the node exists
					// TODO: if it does, we need to rebuild a node
					newEntries := make([]*model.Entry, len(entries))

					copy(newEntries, entries)
					newEntries[ind] = change.Entry
					newNode := &model.Node{
						Entries:  newEntries,
						Pointers: pointers,
					}
					newNodeAddr := ident.Hash(newNode)
					oldNodeAddr := ident.Hash(node)
					r.WriteNode(newNodeAddr, newNode)

					// point parent node to this one instead
					for i := len(visited) - 2; i >= 0; i-- { // -2 since we skip ourselves, we already updated
						parent := nodes[i]
						oldNodeAddr = ident.Hash(parent)
						parentPointers := parent.GetPointers()
						newParentPointers := make([]string, len(parentPointers))
						copy(newParentPointers, parentPointers)
						for pointerIndex, pointer := range parent.GetPointers() {
							if strings.EqualFold(pointer, oldNodeAddr) {
								garbageNode := newParentPointers[pointerIndex]
								r.InvalidateNode(garbageNode) // no longer pointing there
								newParentPointers[pointerIndex] = newNodeAddr
							}
						}
						newNode := &model.Node{
							Entries:  parent.GetEntries(),
							Pointers: newParentPointers,
						}
						newNodeAddr = ident.Hash(newNode)
						r.WriteNode(newNodeAddr, newNode)
					}
					root = newNodeAddr // traversed to the top

				} else if cmp == -1 {
					// changed path is bigger, we need to continue to the next entry
					continue
				} else {
					// change path is smaller, we need to either deep into the tree
					// or if there's no pointer, update this node (splitting if needed, and propagating)
					if len(pointers) >= ind+2 {
						currRoot = pointers[ind+1] // traverse down to the next block
					} else {
						// we're at the correct leaf - we need to insert
						if len(entries) >= branchingFactor {
							// TODO: split
							// what does a split looks like:
							// 1. add entry and sort
							// 2. pick median value, e.g. (1,5,7,9,11) => 7
							// 3. create 2 nodes: one with (1,5) and one with (9,11)
							// 4. write these nodes.
							// 5. insert 7 into the parent node, sorted.
							// 6. insert p(1,5) at 7's index, p(9,11) at 7's+1 index
							// 7. of course, if no space in the parent, recurse the process upwards
						} else {
							// we have space here, let's add
							newEntries := make([]*model.Entry, len(entries))

							copy(newEntries, entries)
							newEntries = append(newEntries, change.Entry)
							sort.SliceStable(newEntries, func(i, j int) bool {
								return strings.Compare(newEntries[i].GetName(), newEntries[j].GetName()) == -1
							})
							newNode := &model.Node{
								Entries:  newEntries,
								Pointers: pointers,
							}
							newNodeAddr := ident.Hash(newNode)
							oldNodeAddr := ident.Hash(node)
							r.WriteNode(newNodeAddr, newNode)

							// point parent node to this one instead
							for i := len(visited) - 2; i >= 0; i-- { // -2 since we skip ourselves, we already updated
								parent := nodes[i]
								oldNodeAddr = ident.Hash(parent)
								parentPointers := parent.GetPointers()
								newParentPointers := make([]string, len(parentPointers))
								copy(newParentPointers, parentPointers)
								for pointerIndex, pointer := range parent.GetPointers() {
									if strings.EqualFold(pointer, oldNodeAddr) {
										garbageNode := newParentPointers[pointerIndex]
										r.InvalidateNode(garbageNode) // no longer pointing there
										newParentPointers[pointerIndex] = newNodeAddr
									}
								}
								newNode := &model.Node{
									Entries:  parent.GetEntries(),
									Pointers: newParentPointers,
								}
								newNodeAddr = ident.Hash(newNode)
								r.WriteNode(newNodeAddr, newNode)
							}
							root = newNodeAddr // traversed to the top
						}
					}
				}
			}
		}

		// update the node
		// if the node is too big, split it into 2 node, propagating the middle node to its parent node
		// repeat until no node exceeds its max size
		// record the new root we received
	}

	err := r.Flush(w) // write all new relevant nodes to storage.
	if err != nil {
		return nil, err
	}

	// return a new tree with the updated root
	return &BTree{db: t.db, root: root}, nil
}
