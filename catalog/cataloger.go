package catalog

import (
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/treeverse/lakefs/db"
	"github.com/treeverse/lakefs/logging"
	"github.com/treeverse/lakefs/retention"
)

const (
	CatalogerCommitter = ""

	DefaultPathDelimiter = "/"

	dedupBatchSize    = 10
	dedupBatchTimeout = 50 * time.Millisecond
	dedupChannelSize  = 5000
)

type DedupResult struct {
	Repository         string
	StorageNamespace   string
	DedupID            string
	Entry              *Entry
	NewPhysicalAddress string
}

type DedupParams struct {
	ID               string
	Ch               chan *DedupResult
	StorageNamespace string
}

type ExpireResult struct {
	Repository      string
	Branch          string
	BranchId        int64 `db:"branch_id"`
	Path            string
	PhysicalAddress string `db:"physical_address"`
	MinCommit       int64  `db:"min_commit"`
}

type RepositoryCataloger interface {
	CreateRepository(ctx context.Context, repository string, storageNamespace string, branch string) error
	GetRepository(ctx context.Context, repository string) (*Repository, error)
	DeleteRepository(ctx context.Context, repository string) error
	ListRepositories(ctx context.Context, limit int, after string) ([]*Repository, bool, error)
}

type BranchCataloger interface {
	CreateBranch(ctx context.Context, repository, branch string, sourceBranch string) error
	DeleteBranch(ctx context.Context, repository, branch string) error
	ListBranches(ctx context.Context, repository string, prefix string, limit int, after string) ([]*Branch, bool, error)
	GetBranchReference(ctx context.Context, repository, branch string) (string, error)
	ResetBranch(ctx context.Context, repository, branch string) error
}

var ErrExpired = errors.New("expired from storage")

// ExpiryRows is a database iterator over ExpiryResults.  Use Next to advance from row to row.
type ExpiryRows interface {
	io.Closer
	Next() bool
	Err() error
	// Read returns the current from ExpiryRows, or an error on failure.  Call it only after
	// successfully calling Next.
	Read() (*ExpireResult, error)
}

type EntryCataloger interface {
	// GetEntry returns the current entry for path in repository branch reference.  Returns
	// the entry with ExpiredError if it has expired from underlying storage.
	GetEntry(ctx context.Context, repository, reference string, path string) (*Entry, error)
	CreateEntry(ctx context.Context, repository, branch string, entry Entry) error
	CreateEntryDedup(ctx context.Context, repository, branch string, entry Entry, dedup DedupParams) error
	CreateEntries(ctx context.Context, repository, branch string, entries []Entry) error
	DeleteEntry(ctx context.Context, repository, branch string, path string) error
	ListEntries(ctx context.Context, repository, reference string, prefix, after string, limit int) ([]*Entry, bool, error)
	ListEntriesByLevel(ctx context.Context, repository, reference, prefix, after, delimiter string, limit int) ([]LevelEntry, bool, error)
	ResetEntry(ctx context.Context, repository, branch string, path string) error
	ResetEntries(ctx context.Context, repository, branch string, prefix string) error
	QueryExpired(ctx context.Context, repositoryName string, policy *retention.Policy) (ExpiryRows, error)
	// MarkExpired marks all entries identified by expire as expired.  It is a batch
	// operation.
	MarkExpired(ctx context.Context, repositoryName string, expireResults []*ExpireResult) error
}

type MultipartUpdateCataloger interface {
	CreateMultipartUpload(ctx context.Context, repository, uploadID, path, physicalAddress string, creationTime time.Time) error
	GetMultipartUpload(ctx context.Context, repository, uploadID string) (*MultipartUpload, error)
	DeleteMultipartUpload(ctx context.Context, repository, uploadID string) error
}

type Committer interface {
	Commit(ctx context.Context, repository, branch string, message string, committer string, metadata Metadata) (*CommitLog, error)
	GetCommit(ctx context.Context, repository, reference string) (*CommitLog, error)
	ListCommits(ctx context.Context, repository, branch string, fromReference string, limit int) ([]*CommitLog, bool, error)
	RollbackCommit(ctx context.Context, repository, reference string) error
}

type Differ interface {
	Diff(ctx context.Context, repository, leftBranch string, rightBranch string) (Differences, error)
	DiffUncommitted(ctx context.Context, repository, branch string) (Differences, error)
}

type MergeResult struct {
	Differences Differences
	Reference   string
}

type Merger interface {
	Merge(ctx context.Context, repository, sourceBranch, destinationBranch string, committer string, message string, metadata Metadata) (*MergeResult, error)
}

type Cataloger interface {
	RepositoryCataloger
	BranchCataloger
	EntryCataloger
	Committer
	MultipartUpdateCataloger
	Differ
	Merger
	io.Closer
}

type DedupFoundCallback func(repository string, dedupID string, previousAddress, newAddress string)

type dedupRequest struct {
	Repository       string
	StorageNamespace string
	DedupID          string
	Entry            *Entry
	EntryCTID        string
	DedupResultCh    chan *DedupResult
}

type CacheConfig struct {
	Enabled bool
	Size    int
	Expiry  time.Duration
	Jitter  time.Duration
}

// cataloger main catalog implementation based on mvcc
type cataloger struct {
	clock       clock.Clock
	log         logging.Logger
	db          db.Database
	dedupCh     chan *dedupRequest
	wg          sync.WaitGroup
	cacheConfig *CacheConfig
	cache       Cache
}

type CatalogerOption func(*cataloger)

var defaultCatalogerCacheConfig = &CacheConfig{
	Enabled: true,
	Size:    1024,
	Expiry:  20 * time.Second,
	Jitter:  5 * time.Second,
}

func WithClock(newClock clock.Clock) CatalogerOption {
	return func(c *cataloger) {
		c.clock = newClock
	}
}

func WithCacheConfig(config *CacheConfig) CatalogerOption {
	return func(c *cataloger) {
		c.cacheConfig = config
	}
}

func NewCataloger(db db.Database, options ...CatalogerOption) Cataloger {
	c := &cataloger{
		clock:       clock.New(),
		log:         logging.Default().WithField("service_name", "cataloger"),
		db:          db,
		dedupCh:     make(chan *dedupRequest, dedupChannelSize),
		cacheConfig: defaultCatalogerCacheConfig,
	}
	for _, opt := range options {
		opt(c)
	}
	if c.cacheConfig.Enabled {
		c.cache = NewLRUCache(c.cacheConfig.Size, c.cacheConfig.Expiry, c.cacheConfig.Jitter)
	} else {
		c.cache = &DummyCache{}
	}
	c.processDedupBatches()
	return c
}

func (c *cataloger) txOpts(ctx context.Context, opts ...db.TxOpt) []db.TxOpt {
	o := []db.TxOpt{
		db.WithContext(ctx),
		db.WithLogger(c.log),
	}
	return append(o, opts...)
}

func (c *cataloger) Close() error {
	if c != nil {
		close(c.dedupCh)
		c.wg.Wait()
	}
	return nil
}

func (c *cataloger) processDedupBatches() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		batch := make([]*dedupRequest, 0, dedupBatchSize)
		timer := time.NewTimer(dedupBatchTimeout)
		for {
			processBatch := false
			select {
			case req, ok := <-c.dedupCh:
				if !ok {
					return
				}
				batch = append(batch, req)
				l := len(batch)
				if l == 1 {
					timer.Reset(dedupBatchTimeout)
				}
				if l == dedupBatchSize {
					processBatch = true
				}
			case <-timer.C:
				if len(batch) > 0 {
					processBatch = true
				}
			}
			if processBatch {
				c.dedupBatch(batch)
				batch = batch[:0]
			}
		}
	}()
}

func (c *cataloger) dedupBatch(batch []*dedupRequest) {
	ctx := context.Background()
	dedupBatchSizeCounter.WithLabelValues(strconv.Itoa(len(batch))).Inc()
	res, err := c.db.Transact(func(tx db.Tx) (interface{}, error) {
		addresses := make([]string, len(batch))
		for i, r := range batch {
			repoID, err := c.getRepositoryIDCache(tx, r.Repository)
			if err != nil {
				return nil, err
			}

			// add dedup record
			res, err := tx.Exec(`INSERT INTO object_dedup (repository_id, dedup_id, physical_address) values ($1, decode($2,'hex'), $3)
				ON CONFLICT DO NOTHING`,
				repoID, r.DedupID, r.Entry.PhysicalAddress)
			if err != nil {
				return nil, err
			}
			if rowsAffected, err := res.RowsAffected(); err != nil {
				return nil, err
			} else if rowsAffected == 1 {
				// new address was added - continue
				continue
			}

			// fill the address into the right location
			err = tx.Get(&addresses[i], `SELECT physical_address FROM object_dedup WHERE repository_id=$1 AND dedup_id=decode($2,'hex')`,
				repoID, r.DedupID)
			if err != nil {
				return nil, err
			}

			// update the entry with new address physical address
			_, err = tx.Exec(`UPDATE entries SET physical_address=$2 WHERE ctid=$1 AND physical_address=$3`,
				r.EntryCTID, addresses[i], r.Entry.PhysicalAddress)
			if err != nil {
				return nil, err
			}
		}
		return addresses, nil
	}, c.txOpts(ctx)...)
	if err != nil {
		c.log.WithError(err).Errorf("Dedup batch failed (%d requests)", len(batch))
		return
	}

	// call callbacks for each entry we updated
	addresses := res.([]string)
	for i, r := range batch {
		if r.DedupResultCh != nil {
			r.DedupResultCh <- &DedupResult{
				Repository:         r.Repository,
				StorageNamespace:   r.StorageNamespace,
				Entry:              r.Entry,
				DedupID:            r.DedupID,
				NewPhysicalAddress: addresses[i],
			}
		}
	}
}
