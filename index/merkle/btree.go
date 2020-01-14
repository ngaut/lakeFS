package merkle

import (
	"sort"
	"strings"
	"treeverse-lake/db"
	"treeverse-lake/ident"
	"treeverse-lake/index/model"

	"golang.org/x/xerrors"
)

type CachedNodeReader struct {
	db          BTreeReader
	cache       map[string]*model.Node
	dirtyWrites map[string]*model.Node
}

func (c *CachedNodeReader) GetNode(addr string) (*model.Node, error) {
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

func (c *CachedNodeReader) WriteNode(addr string, node *model.Node) error {
	c.cache[addr] = node
	c.dirtyWrites[addr] = node
	return nil
}

func NewCacheNodeReader(db BTreeReader) *CachedNodeReader {
	return &CachedNodeReader{
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

// TODO: write path - return a new BTree on successful change
func (t *BTree) Update(w BTreeWriter, branchingFactor int, changes []*TreeChange) (*BTree, error) {
	root := t.root
	nodes := make([]*model.Node, 0)
	// algorithm
	// - for every change we do:
	// - - traverse the tree to find the node that should contain the change
	// - - make the change in the given node
	// - - go to the parent - reflect the changes that need to happen - at a minimum - a pointer has changed
	// - - this ^ happens recursively, until we get to the root node. This means we're creating a new root node for every modification
	// once all changes are done, a lot of the internal nodes we created are no longer required - what is the rule?
	// we can keep a reference count - every time we set a pointer to a node, pointerHash++
	// on the other hand, every time we replace an existing pointer, pointerHash--
	// decrementing to zero will "remove" this node from the list of nodes that need to be written eventually
	// this is efficient since we only once scan the list of nodes so its o(n)

	// once all changes have been reflected in the tree we write all nodes that still exist within the list
	// do we really need to incr/decr? it's either one pointer or none since its ordered paths
	// we can keep a set and simply add(x)/del(x) for previous pointers. we also need to do this for the root node!

	// so to recap:
	// - we make 1 change at a time, propagating changes up the tree and replacing the root node every time
	// - once we've reflected all changes, and have the logical nodes built, we write the nodes to DB
	// - at the end, return the last root generated and written

	// I still think this is a garbage implementation and it's super complex but imma try to implement this shizz
	nodecache := make(map[string]*model.Node)

	// use a cached reader to avoid getting the same nodes all the time
	r := NewCacheNodeReader(t.db)

	// sort changes by path to ensure minimal retrievals and updates
	sort.SliceStable(changes, func(i, j int) bool {
		return strings.Compare(changes[i].Path, changes[j].Path) == -1
	})

	// calculate the new nodes
	for _, change := range changes {
		// find the node that should contain this change
		currRoot := t.root
		for {
			node, err := r.GetNode(currRoot)
			if err != nil {
				return nil, err
			}
			pointers := node.GetPointers()
			for ind, entry := range node.GetEntries() {
				cmp := strings.Compare(change.Path, entry.GetName())
				if cmp == 0 {
					// this is the node - we need to replace whatever value we had with the current change
					// and propagate this up the tree resulting in a new root
				} else if cmp == -1 {
					// changed path is bigger, we need to continue
				} else {
					// change path is smaller, we need to either deep into the tree
					// or if there's no pointer, update this node (splitting if needed, and propagating)
				}
			}
		}

		// update the node
		// if the node is too big, split it into 2 node, propagating the middle node to its parent node
		// repeat until no node exceeds its max size
		// record the new root we received
	}

	// persist all new nodes
	for _, node := range nodes {
		err := w.WriteNode(ident.Hash(node), node)
		if err != nil {
			return nil, err
		}
	}

	return &BTree{db: t.db, root: root}, nil
}
