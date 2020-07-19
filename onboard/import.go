package onboard

import (
	"context"
	"fmt"
	"github.com/treeverse/lakefs/block"
	"github.com/treeverse/lakefs/catalog"
	"time"
)

const (
	DefaultBranchName = "import-from-inventory"
	CommitMsgTemplate = "Import from %s"
	DefaultBatchSize  = 500
)

type Importer struct {
	repository         string
	batchSize          int
	inventoryGenerator block.InventoryGenerator
	inventory          block.Inventory
	InventoryDiffer    func(leftInv []block.InventoryObject, rightInv []block.InventoryObject) *InventoryDiff
	CatalogActions     RepoActions
}

type InventoryImportStats struct {
	AddedOrChanged       int
	Deleted              int
	DryRun               bool
	PreviousInventoryURL string
	PreviousImportDate   time.Time
}

type ObjectImport struct {
	obj      block.InventoryObject
	toDelete bool
}

func CreateImporter(cataloger catalog.Cataloger, inventoryGenerator block.InventoryGenerator, username string, inventoryURL string, repository string) (importer *Importer, err error) {
	res := &Importer{
		repository:         repository,
		batchSize:          DefaultBatchSize,
		inventoryGenerator: inventoryGenerator,
		InventoryDiffer:    CalcDiff,
	}
	res.inventory, err = inventoryGenerator.GenerateInventory(inventoryURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create inventory: %w", err)
	}
	res.CatalogActions = NewCatalogActions(cataloger, repository, username, DefaultBatchSize)
	return res, nil
}

func (s *Importer) diffFromCommit(ctx context.Context, commit catalog.CommitLog) (diff *InventoryDiff, err error) {
	previousInventoryURL := ExtractInventoryURL(commit.Metadata)
	if previousInventoryURL == "" {
		err = fmt.Errorf("no inventory_url in commit Metadata. commit_ref=%s", commit.Reference)
		return
	}
	previousInv, err := s.inventoryGenerator.GenerateInventory(previousInventoryURL)
	if err != nil {
		err = fmt.Errorf("failed to create inventory for previous state: %w", err)
		return
	}
	previousObjs, err := previousInv.Objects(ctx, true)
	if err != nil {
		return
	}
	currentObjs, err := s.inventory.Objects(ctx, true)
	if err != nil {
		return
	}
	diff = s.InventoryDiffer(previousObjs, currentObjs)
	diff.PreviousInventoryURL = previousInventoryURL
	diff.PreviousImportDate = commit.CreationDate
	return
}

func (s *Importer) Import(ctx context.Context, dryRun bool) (*InventoryImportStats, error) {
	var stats InventoryImportStats
	in, err := s.dataToImportChannel(ctx)
	if err != nil {
		return nil, err
	}
	batch := make([]ObjectImport, 0, 1000)
	for objectImport := range in {
		batch = append(batch, objectImport)
	}
	stats.DryRun = dryRun
	if dryRun {
		//return diff, nil
	}
	err = s.CatalogActions.CreateAndDeleteObjects(ctx, diff.AddedOrChanged, diff.Deleted)
	if err != nil {
		return nil, err
	}
	commitMetadata := CreateCommitMetadata(s.inventory, *diff)
	err = s.CatalogActions.Commit(ctx, fmt.Sprintf(CommitMsgTemplate, s.inventory.SourceName()), commitMetadata)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (s *Importer) dataToImport(ctx context.Context) (diff *InventoryDiff, err error) {
	var commit *catalog.CommitLog
	commit, err = s.CatalogActions.GetPreviousCommit(ctx)
	if err != nil {
		return
	}
	if commit == nil {
		// no previous commit, add whole inventory
		var objects []block.InventoryObject
		objects, err = s.inventory.Objects(ctx, false)
		if err != nil {
			return
		}
		diff = &InventoryDiff{AddedOrChanged: objects}
	} else {
		// has previous commit, add/delete according to diff
		diff, err = s.diffFromCommit(ctx, *commit)
		if err != nil {
			return nil, err
		}
	}
	return
}

func (s *Importer) dataToImportChannel(ctx context.Context) (<-chan ObjectImport, error) {
	commit, err := s.CatalogActions.GetPreviousCommit(ctx)
	if err != nil {
		return nil, err
	}
	out := make(chan ObjectImport)
	if commit == nil {
		// no previous commit, add whole inventory
		in, err := s.inventory.ObjectsChannel(ctx)
		if err != nil {
			return nil, err
		}

		go func() {
			for o := range in {
				out <- ObjectImport{
					obj: o,
				}
			}
		}()
	} else {
		// TODO handle diff
	}
	return out, nil
}
