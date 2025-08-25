package unitow

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"
)

const (
	CONTEXT_FORCE_ROLLBACK = "force_rollback"
)

type contextKey string

var (
	unitowKey                = contextKey("unitow")
	contextCriticalStatusKey = contextKey("context_critical_status")
)

type Unitow interface {
	NewTransaction(name string) Transaction
}

type Transaction interface {
	Start()
	Commit() (bool, error)
	CommitAll() error
	Rollback()
	GetDb() Database
	Transaction() Database
	SetCriticalStatus(status string)
}

type Database interface {
	Commit() error
	Rollback() error
	Begin() Database
}

// Logger provides minimal leveled logging.
type Logger interface {
	Debug(msg string, fields ...any)
	Info(msg string, fields ...any)
	Warn(msg string, fields ...any)
	Error(msg string, fields ...any)
}

type noopLogger struct{}

func (l noopLogger) Debug(string, ...any) {}
func (l noopLogger) Info(string, ...any)  {}
func (l noopLogger) Warn(string, ...any)  {}
func (l noopLogger) Error(string, ...any) {}

// Metrics captures basic counters for observability.
type Metrics interface {
	IncBegin()
	IncCommit()
	IncRollback()
}

type noopMetrics struct{}

func (m noopMetrics) IncBegin()    {}
func (m noopMetrics) IncCommit()   {}
func (m noopMetrics) IncRollback() {}

// Hooks are optional callbacks around lifecycle.
type Hooks struct {
	BeforeCommit  func(name string)
	AfterCommit   func(name string)
	AfterRollback func(name string)
}

// TransactionOptions controls behavior of a transaction instance.
type TransactionOptions struct {
	Context       context.Context
	Timeout       time.Duration
	ReadOnly      bool
	RetryAttempts int
	RetryBackoff  time.Duration
}

type unitow struct {
	db           Database
	transactions *sync.Map
	signalChan   chan string
	logger       Logger
	hooks        Hooks
	metrics      Metrics
	defaultOpts  TransactionOptions
}

func New(db Database) Unitow {
	uni := &unitow{
		db:           db,
		transactions: &sync.Map{},
		signalChan:   make(chan string),
		logger:       noopLogger{},
		metrics:      noopMetrics{},
	}

	go uni.listenToSignal()

	return uni

}

// NewWith allows providing logger, hooks and metrics. Nil values are replaced with no-ops.
func NewWith(db Database, logger Logger, hooks Hooks, metrics Metrics, defaultOpts TransactionOptions) Unitow {
	if logger == nil {
		logger = noopLogger{}
	}
	if metrics == nil {
		metrics = noopMetrics{}
	}
	uni := &unitow{
		db:           db,
		transactions: &sync.Map{},
		signalChan:   make(chan string),
		logger:       logger,
		hooks:        hooks,
		metrics:      metrics,
		defaultOpts:  defaultOpts,
	}
	go uni.listenToSignal()
	return uni
}

func (u *unitow) listenToSignal() {
	for key := range u.signalChan {
		u.transactions.Delete(key)
	}
}

type transaction struct {
	db             Database
	tx             Database
	count          atomic.Int32
	active         atomic.Bool
	signalChan     chan string
	name           string
	criticalStatus string
	logger         Logger
	hooks          Hooks
	metrics        Metrics
	options        TransactionOptions
	committed      atomic.Bool
	rolledBack     atomic.Bool
	cancelTimer    func()
}

func (u *unitow) NewTransaction(name string) Transaction {

	value, found := u.transactions.Load(name)
	if found {
		u.logger.Debug("reusing transaction", "name", name)
		return value.(Transaction)
	}

	trx := &transaction{
		db:         u.db,
		signalChan: u.signalChan,
		name:       name,
		logger:     u.logger,
		hooks:      u.hooks,
		metrics:    u.metrics,
		options:    u.defaultOpts,
	}

	u.transactions.Store(name, trx)

	//u.logger.Debug(fmt.Sprintf("create new transaction with name: %s", name))
	return trx

}

// NewTransactionWithOptions creates a transaction with explicit options.
func (u *unitow) NewTransactionWithOptions(name string, opts TransactionOptions) Transaction {
	value, found := u.transactions.Load(name)
	if found {
		u.logger.Debug("reusing transaction", "name", name)
		return value.(Transaction)
	}
	trx := &transaction{
		db:         u.db,
		signalChan: u.signalChan,
		name:       name,
		logger:     u.logger,
		hooks:      u.hooks,
		metrics:    u.metrics,
		options:    opts,
	}
	u.transactions.Store(name, trx)
	return trx
}

func (u *transaction) Start() {
	if !u.active.Load() {
		u.tx = u.db.Begin()
		u.active.Store(true)
		u.metrics.IncBegin()
		// setup timeout/cancellation watcher if configured
		ctx := u.options.Context
		if ctx == nil {
			ctx = context.Background()
		}
		if u.options.Timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, u.options.Timeout)
			u.cancelTimer = cancel
		}
		// watcher goroutine
		go func(name string, done <-chan struct{}) {
			select {
			case <-done:
				u.logger.Warn("transaction context cancelled or timed out", "name", name)
				u.SetCriticalStatus(CONTEXT_FORCE_ROLLBACK)
			default:
			}
		}(u.name, ctx.Done())
	}
	u.count.Add(1)
}

func (u *transaction) Commit() (bool, error) {

	if u.criticalStatus == CONTEXT_FORCE_ROLLBACK {
		u.logger.Warn("transaction forced to rollback; cannot commit", "name", u.name)
		u.tx.Rollback()
		u.metrics.IncRollback()
		if u.hooks.AfterRollback != nil {
			u.hooks.AfterRollback(u.name)
		}
		return true, nil
	}

	if u.active.Load() {
		u.count.Add(-1)
		if u.count.Load() <= 0 {

			defer func() {
				u.signalChan <- u.name
			}()

			if u.rolledBack.Load() {
				return true, nil
			}
			if u.committed.Load() {
				return true, nil
			}

			if u.hooks.BeforeCommit != nil {
				u.hooks.BeforeCommit(u.name)
			}

			attempts := u.options.RetryAttempts
			if attempts <= 0 {
				attempts = 1
			}
			backoff := u.options.RetryBackoff
			var err error
			for i := 0; i < attempts; i++ {
				err = u.tx.Commit()
				if err == nil {
					break
				}
				if i < attempts-1 && backoff > 0 {
					time.Sleep(backoff)
				}
			}
			u.active.Store(false)
			u.count.Store(0)
			if u.cancelTimer != nil {
				u.cancelTimer()
			}

			if err != nil {
				u.logger.Error("commit failed", "name", u.name, "error", err)
				return false, err
			}

			u.committed.Store(true)
			u.metrics.IncCommit()
			if u.hooks.AfterCommit != nil {
				u.hooks.AfterCommit(u.name)
			}
			return true, nil
		} else {
			u.logger.Debug("open nested transactions remain", "open", u.count.Load())
			return false, nil

		}
	}
	return false, errors.New("we are committing on a deactive transaction")
}

func (u *transaction) SetCriticalStatus(status string) {
	if len(status) != 0 {

		u.logger.Info("transaction critical status set", "name", u.name, "status", status)
	}
	u.criticalStatus = status
}

func (u *transaction) CommitAll() error {
	if u.criticalStatus == CONTEXT_FORCE_ROLLBACK {
		u.logger.Warn("transaction forced to rollback; cannot commit", "name", u.name)
		u.tx.Rollback()
		u.metrics.IncRollback()
		if u.hooks.AfterRollback != nil {
			u.hooks.AfterRollback(u.name)
		}
		return nil
	}
	for {
		finish, err := u.Commit()
		if err != nil {
			return err
		} else if finish {
			return nil
		}
	}
}

func (u *transaction) Rollback() {
	if u.active.Load() {

		defer func() {
			u.signalChan <- u.name
		}()

		u.tx.Rollback()
		u.active.Store(false)
		u.count.Store(0)
		u.rolledBack.Store(true)
		if u.cancelTimer != nil {
			u.cancelTimer()
		}
		u.metrics.IncRollback()
		if u.hooks.AfterRollback != nil {
			u.hooks.AfterRollback(u.name)
		}
	}
}

func (u *transaction) GetDb() Database {

	return u.db
}

func (u *transaction) Transaction() Database {
	if u.active.Load() {
		return u.tx
	}

	return u.db
}

func AddToContext(ctx context.Context, uow Transaction) (context.Context, error) {
	fromContext := GetFromContext(ctx)
	if fromContext != nil {
		return nil, errors.New("we already have a transaction on context")
	}
	uow.SetCriticalStatus(GetContextCriticalStatus(ctx))

	uow.Start()
	return context.WithValue(ctx, unitowKey, uow), nil
}

func GetFromContext(ctx context.Context) Transaction {
	uow, ok := ctx.Value(unitowKey).(Transaction)
	if !ok || uow == nil {
		return nil
	}
	uow.SetCriticalStatus(GetContextCriticalStatus(ctx))
	return uow
}

func RollBack(ctx context.Context) error {
	uow := GetFromContext(ctx)
	if uow == nil {
		return errors.New("we don't have any transaction on the context")
	}
	uow.Rollback()
	return nil
}

func AutoCommit(ctx context.Context, err error) error {

	uow := GetFromContext(ctx)
	if uow == nil {
		return errors.New("we don't have any transaction on the context")
	}

	uow.SetCriticalStatus(GetContextCriticalStatus(ctx))

	if r := recover(); r != nil {
		uow.Rollback()
		return errors.New("panic happened, rolling back")
	} else if err != nil {
		if errTransaction, ok := err.(TransactionError); !ok {
			uow.Rollback()
		} else {
			if errTransaction.DoCommit() {
				errCommitAll := uow.CommitAll()
				if errCommitAll != nil {
					return errCommitAll
				}
			} else {
				uow.Rollback()
			}
		}
	} else {
		errCommitAll := uow.CommitAll()
		if errCommitAll != nil {
			return errCommitAll
		}
	}

	return nil
}

func SetContextCriticalStatusToForceRollback(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextCriticalStatusKey, CONTEXT_FORCE_ROLLBACK)
}

func GetContextCriticalStatus(ctx context.Context) string {

	v := ctx.Value(contextCriticalStatusKey)
	if v != nil {

		switch x := v.(type) {

		case string:
			return x
		default:
			fmt.Printf("CONTEXT_CRITICAL_STATUS isnt string!!! %v", v)
			return ""
		}

	}
	return ""
}

func extractFunctionName(input string) string {
	parts := strings.Split(input, ".")
	return parts[len(parts)-1]
}

func caller(skip int) runtime.Frame {
	pc := make([]uintptr, 15)
	n := runtime.Callers(skip, pc)
	frames := runtime.CallersFrames(pc[:n])
	frame, _ := frames.Next()

	return frame

}

func GenerateName(skipLevel int) string {

	return fmt.Sprintf("%v_%v", ulid.Make().String(), extractFunctionName(caller(skipLevel).Function))
}

type TransactionError interface {
	DoCommit() bool
}
