// Package writer consumes location messages from Kafka and persists them to Postgres.
//
// The worker owns the Kafka reader lifecycle, batches incoming messages, writes
// them via a single UNNEST insert, and only commits offsets after the DB write
// succeeds. Messages with unsupported versions are logged and skipped.
package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/TheRealShek/trackr7/db"
	"github.com/TheRealShek/trackr7/schema"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

const (
	defaultBatchSize  = 500
	defaultFlushEvery = time.Second
	workerGroupPrefix = "trackr7.writer."
	messageVersion    = 1
)

// Config configures a writer worker.
type Config struct {
	// KafkaBrokers lists bootstrap brokers for the reader.
	KafkaBrokers []string
	// KafkaDialer configures TLS/SASL if needed.
	KafkaDialer *kafka.Dialer
	// Topic is the Kafka topic to consume.
	Topic string
	// Namespace scopes the consumer group ID.
	Namespace string
	// DB is the Postgres pool used for writes.
	DB *pgxpool.Pool
	// DBConfig controls table and column names for inserts.
	DBConfig db.DBConfig
	// DBTimeout bounds batch DB writes when the caller context has no deadline.
	// 0 = no library-enforced timeout.
	DBTimeout time.Duration
	// BatchSize is the max number of messages per flush.
	BatchSize int
	// FlushEvery is the max time between flushes.
	FlushEvery time.Duration
	// Logger records operational messages. Nil-safe.
	Logger schema.Logger
	// Observability hooks — optional, nil-safe.
	// OnBatchFlushed fires after a successful non-empty flush.
	OnBatchFlushed func(count int, duration time.Duration)
	// OnFlushError fires when a batch write or commit fails.
	OnFlushError func(err error)
	// OnMessageSkipped fires when a message is skipped (e.g., unsupported version).
	OnMessageSkipped func(reason string)
}

// Worker consumes Kafka messages and persists them to Postgres.
type Worker struct {
	reader           *kafka.Reader
	db               *pgxpool.Pool
	dbConfig         db.DBConfig
	dbTimeout        time.Duration
	insertSQL        string
	batchSize        int
	flushEvery       time.Duration
	logger           schema.Logger
	onBatchFlushed   func(count int, duration time.Duration)
	onFlushError     func(err error)
	onMessageSkipped func(reason string)
}

type rawLocationMessage struct {
	UUID       string  `json:"uuid"`
	EntityID   string  `json:"entity_id"`
	EntityType string  `json:"entity_type"`
	Lat        float64 `json:"lat"`
	Lng        float64 `json:"lng"`
	TS         int64   `json:"ts"`
	V          int     `json:"v"`
}

type batchItem struct {
	message kafka.Message
	row     rawLocationMessage
	write   bool
}

type fetchResult struct {
	message kafka.Message
	err     error
}

type batchPlan struct {
	commitMessages []kafka.Message
	rows           []rawLocationMessage
}

// NewWorker creates a new Kafka-to-Postgres worker.
func NewWorker(cfg Config) (*Worker, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("%w: DB pool is nil", schema.ErrInvalidConfig)
	}
	if strings.TrimSpace(cfg.Namespace) == "" {
		return nil, fmt.Errorf("%w: Namespace is required", schema.ErrNamespaceRequired)
	}
	if len(cfg.KafkaBrokers) == 0 {
		return nil, fmt.Errorf("%w: KafkaBrokers is empty", schema.ErrInvalidConfig)
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, fmt.Errorf("%w: Topic is empty", schema.ErrInvalidConfig)
	}

	dbConfig := cfg.DBConfig.WithDefaults()
	dbConfig.Pool = cfg.DB
	if err := dbConfig.Validate(); err != nil {
		return nil, err
	}

	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	flushEvery := cfg.FlushEvery
	if flushEvery <= 0 {
		flushEvery = defaultFlushEvery
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.KafkaBrokers,
		GroupID:     workerGroupPrefix + cfg.Namespace,
		GroupTopics: []string{cfg.Topic},
		Dialer:      cfg.KafkaDialer,
	})

	worker := &Worker{
		reader:           reader,
		db:               cfg.DB,
		dbConfig:         dbConfig,
		dbTimeout:        cfg.DBTimeout,
		insertSQL:        buildInsertSQL(dbConfig),
		batchSize:        batchSize,
		flushEvery:       flushEvery,
		logger:           schema.SafeLogger(cfg.Logger),
		onBatchFlushed:   cfg.OnBatchFlushed,
		onFlushError:     cfg.OnFlushError,
		onMessageSkipped: cfg.OnMessageSkipped,
	}

	return worker, nil
}

// Run consumes messages until ctx is canceled or the reader fails.
func (w *Worker) Run(ctx context.Context) error {
	if w == nil {
		return fmt.Errorf("%w: worker is nil", schema.ErrInvalidConfig)
	}

	fetchCtx, cancelFetch := context.WithCancel(ctx)
	defer cancelFetch()
	defer w.reader.Close()

	results := make(chan fetchResult, 1)
	go w.fetchLoop(fetchCtx, results)

	timer := time.NewTimer(w.flushEvery)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	flushCtx := ctx
	batch := make([]batchItem, 0, w.batchSize)
	timerActive := false
	shutdown := false

	for {
		if ctx.Err() != nil && !shutdown {
			shutdown = true
			flushCtx = context.Background()
			cancelFetch()
			stopTimer(timer)
			timerActive = false
		}

		if shutdown {
			result, ok := <-results
			if !ok {
				if len(batch) > 0 {
					if err := w.flush(flushCtx, batch); err != nil {
						return err
					}
				}
				return ctx.Err()
			}
			if result.err != nil {
				continue
			}
			item, err := w.classifyMessage(result.message)
			if err != nil {
				return err
			}
			batch = append(batch, item)
			if len(batch) >= w.batchSize {
				if err := w.flush(flushCtx, batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
			continue
		}

		select {
		case <-ctx.Done():
			shutdown = true
			flushCtx = context.Background()
			cancelFetch()
			stopTimer(timer)
			timerActive = false
		case result, ok := <-results:
			if !ok {
				if len(batch) > 0 {
					if err := w.flush(flushCtx, batch); err != nil {
						return err
					}
				}
				return nil
			}
			if result.err != nil {
				if len(batch) > 0 {
					if err := w.flush(flushCtx, batch); err != nil {
						return err
					}
				}
				return result.err
			}

			item, err := w.classifyMessage(result.message)
			if err != nil {
				return err
			}
			batch = append(batch, item)
			if !timerActive {
				timer.Reset(w.flushEvery)
				timerActive = true
			}
			if len(batch) >= w.batchSize {
				if err := w.flush(flushCtx, batch); err != nil {
					return err
				}
				batch = batch[:0]
				stopTimer(timer)
				timerActive = false
			}
		case <-timer.C:
			if len(batch) > 0 {
				if err := w.flush(flushCtx, batch); err != nil {
					return err
				}
				batch = batch[:0]
			}
			stopTimer(timer)
			timerActive = false
		}
	}
}

func (w *Worker) fetchLoop(ctx context.Context, out chan<- fetchResult) {
	defer close(out)

	for {
		message, err := w.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case out <- fetchResult{err: err}:
			case <-ctx.Done():
			}
			return
		}

		select {
		case out <- fetchResult{message: message}:
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) classifyMessage(message kafka.Message) (batchItem, error) {
	var payload rawLocationMessage
	if err := json.Unmarshal(message.Value, &payload); err != nil {
		return batchItem{}, fmt.Errorf("decode kafka message: %w", err)
	}

	item := batchItem{
		message: message,
		row:     payload,
		write:   true,
	}

	if payload.V != messageVersion {
		w.logger.Info("skipping unsupported location message version", "version", payload.V, "uuid", payload.UUID, "entity_id", payload.EntityID)
		w.safeCallOnMessageSkipped("unsupported_version")
		item.write = false
	}

	return item, nil
}

func (w *Worker) flush(ctx context.Context, batch []batchItem) error {
	if len(batch) == 0 {
		return nil
	}

	start := time.Now()
	plan := buildBatchPlan(batch)

	if len(plan.rows) > 0 {
		if err := w.writeBatch(ctx, plan.rows); err != nil {
			w.safeCallOnFlushError(err)
			return err
		}
	}

	if len(plan.commitMessages) == 0 {
		return nil
	}

	if err := w.reader.CommitMessages(ctx, plan.commitMessages...); err != nil {
		w.safeCallOnFlushError(err)
		return fmt.Errorf("commit kafka offsets: %w", err)
	}

	if len(plan.rows) > 0 {
		w.safeCallOnBatchFlushed(len(plan.rows), time.Since(start))
	}

	return nil
}

// safe hook callers that recover from panics and log via worker logger
func (w *Worker) safeCallOnBatchFlushed(count int, duration time.Duration) {
	if w.onBatchFlushed == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("OnBatchFlushed hook panicked", "panic", r)
		}
	}()
	w.onBatchFlushed(count, duration)
}

func (w *Worker) safeCallOnFlushError(err error) {
	if w.onFlushError == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("OnFlushError hook panicked", "panic", r)
		}
	}()
	w.onFlushError(err)
}

func (w *Worker) safeCallOnMessageSkipped(reason string) {
	if w.onMessageSkipped == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("OnMessageSkipped hook panicked", "panic", r)
		}
	}()
	w.onMessageSkipped(reason)
}

func (w *Worker) writeBatch(ctx context.Context, rows []rawLocationMessage) error {
	if len(rows) == 0 {
		return nil
	}

	uuids := make([]string, 0, len(rows))
	entityIDs := make([]string, 0, len(rows))
	entityTypes := make([]string, 0, len(rows))
	lats := make([]float64, 0, len(rows))
	lngs := make([]float64, 0, len(rows))
	ts := make([]int64, 0, len(rows))

	for _, row := range rows {
		uuids = append(uuids, row.UUID)
		entityIDs = append(entityIDs, row.EntityID)
		entityTypes = append(entityTypes, row.EntityType)
		lats = append(lats, row.Lat)
		lngs = append(lngs, row.Lng)
		ts = append(ts, row.TS)
	}

	callCtx := ctx
	if w.dbTimeout > 0 {
		if _, hasDeadline := ctx.Deadline(); !hasDeadline {
			var cancel context.CancelFunc
			callCtx, cancel = context.WithTimeout(ctx, w.dbTimeout)
			defer cancel()
		}
	}

	_, err := w.db.Exec(callCtx, w.insertSQL, uuids, entityIDs, entityTypes, lats, lngs, ts)
	if err != nil {
		return fmt.Errorf("insert locations batch: %w", err)
	}

	return nil
}

func buildBatchPlan(batch []batchItem) batchPlan {
	plan := batchPlan{
		commitMessages: make([]kafka.Message, 0, len(batch)),
		rows:           make([]rawLocationMessage, 0, len(batch)),
	}

	for _, item := range batch {
		plan.commitMessages = append(plan.commitMessages, item.message)
		if item.write {
			plan.rows = append(plan.rows, item.row)
		}
	}

	return plan
}

func buildInsertSQL(cfg db.DBConfig) string {
	cols := cfg.LocationColumns
	return fmt.Sprintf(
		"INSERT INTO %s (%s, %s, %s, %s, %s, %s) SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::float8[], $5::float8[], $6::bigint[]) ON CONFLICT (%s) DO NOTHING",
		cfg.LocationsTable,
		cols.UUID,
		cols.EntityID,
		cols.EntityType,
		cols.Lat,
		cols.Lng,
		cols.TS,
		cols.UUID,
	)
}

func stopTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}
