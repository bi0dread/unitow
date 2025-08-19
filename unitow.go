package unitow

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

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

type unitow struct {
	db           Database
	transactions *sync.Map
	signalChan   chan string
}

func New(db Database) Unitow {
	uni := &unitow{
		db:           db,
		transactions: &sync.Map{},
		signalChan:   make(chan string),
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
}

func (u *unitow) NewTransaction(name string) Transaction {

	value, found := u.transactions.Load(name)
	if found {
		fmt.Printf("we found a transaction with name: %s , we return it!!!\n", name)
		return value.(Transaction)
	}

	trx := &transaction{
		db:         u.db,
		signalChan: u.signalChan,
		name:       name,
	}

	u.transactions.Store(name, trx)

	//u.logger.Debug(fmt.Sprintf("create new transaction with name: %s", name))
	return trx

}

func (u *transaction) Start() {
	if !u.active.Load() {
		u.tx = u.db.Begin()
		u.active.Store(true)
	}
	u.count.Add(1)
}

func (u *transaction) Commit() (bool, error) {

	if u.criticalStatus == CONTEXT_FORCE_ROLLBACK {
		fmt.Println("transaction force to rollback , we can't commit it!!!")
		u.tx.Rollback()
		return true, nil
	}

	if u.active.Load() {
		u.count.Add(-1)
		if u.count.Load() <= 0 {

			defer func() {
				u.signalChan <- u.name
			}()

			err := u.tx.Commit()
			u.active.Store(false)
			u.count.Store(0)

			return err == nil, err
		} else {
			fmt.Printf("we have %d open transactions yet\n", u.count.Load())
			return false, nil

		}
	}
	return false, errors.New("we are committing on a deactive transaction")
}

func (u *transaction) SetCriticalStatus(status string) {
	if len(status) != 0 {

		fmt.Println("transaction CriticalStatus set to " + status)
	}
	u.criticalStatus = status
}

func (u *transaction) CommitAll() error {
	if u.criticalStatus == CONTEXT_FORCE_ROLLBACK {
		fmt.Println("transaction force to rollback , we can't commit it")
		u.tx.Rollback()
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
