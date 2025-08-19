package unitow

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

type mockMetrics struct {
	begins    int64
	commits   int64
	rollbacks int64
}

type MockDB struct {
	metrics     *mockMetrics
	commitErr   error
	rollbackErr error
}

func NewMockDB() *MockDB {
	return &MockDB{metrics: &mockMetrics{}}
}

func (m *MockDB) Begin() Database {
	m.metrics.begins++
	return &MockDB{metrics: m.metrics, commitErr: m.commitErr, rollbackErr: m.rollbackErr}
}

func (m *MockDB) Commit() error {
	m.metrics.commits++
	return m.commitErr
}

func (m *MockDB) Rollback() error {
	m.metrics.rollbacks++
	return m.rollbackErr
}

func TestNewTransactionReuseAndCleanup(t *testing.T) {
	db := NewMockDB()
	u := New(db)
	name := "trx-reuse"

	trx1 := u.NewTransaction(name)
	trx2 := u.NewTransaction(name)

	if reflect.ValueOf(trx1).Pointer() != reflect.ValueOf(trx2).Pointer() {
		t.Fatalf("expected same transaction instance for same name before cleanup")
	}

	trx1.Start()
	trx1.Start()

	finish, err := trx1.Commit()
	if err != nil || finish {
		t.Fatalf("expected first commit to be partial, got finish=%v err=%v", finish, err)
	}

	finish, err = trx1.Commit()
	if err != nil || !finish {
		t.Fatalf("expected final commit to finish, got finish=%v err=%v", finish, err)
	}

	if db.metrics.commits != 1 {
		t.Fatalf("expected 1 commit on db, got %d", db.metrics.commits)
	}

	// Wait for async cleanup to remove the transaction from the map
	var trx3 Transaction
	for i := 0; i < 50; i++ {
		trx3 = u.NewTransaction(name)
		if reflect.ValueOf(trx1).Pointer() != reflect.ValueOf(trx3).Pointer() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if reflect.ValueOf(trx1).Pointer() == reflect.ValueOf(trx3).Pointer() {
		t.Fatalf("expected a new transaction instance after final commit and cleanup")
	}
}

func TestRollback(t *testing.T) {
	db := NewMockDB()
	u := New(db)
	name := "trx-rollback"

	trx := u.NewTransaction(name)
	trx.Start()
	trx.Rollback()

	if db.metrics.rollbacks != 1 {
		t.Fatalf("expected 1 rollback on db, got %d", db.metrics.rollbacks)
	}

	// After rollback, a new transaction instance should be created
	var newTrx Transaction
	for i := 0; i < 50; i++ {
		newTrx = u.NewTransaction(name)
		if reflect.ValueOf(trx).Pointer() != reflect.ValueOf(newTrx).Pointer() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if reflect.ValueOf(trx).Pointer() == reflect.ValueOf(newTrx).Pointer() {
		t.Fatalf("expected a new transaction instance after rollback and cleanup")
	}
}

func TestForceRollbackInCommit(t *testing.T) {
	db := NewMockDB()
	u := New(db)
	trx := u.NewTransaction("force-rollback")
	trx.Start()
	trx.SetCriticalStatus(CONTEXT_FORCE_ROLLBACK)

	finish, err := trx.Commit()
	if err != nil || !finish {
		t.Fatalf("expected force rollback path to finish without error, got finish=%v err=%v", finish, err)
	}
	if db.metrics.rollbacks != 1 {
		t.Fatalf("expected rollback to be called once, got %d", db.metrics.rollbacks)
	}
	if db.metrics.commits != 0 {
		t.Fatalf("did not expect commit to be called, got %d", db.metrics.commits)
	}
}

func TestContextHelpers_AddGetRollback(t *testing.T) {
	db := NewMockDB()
	u := New(db)
	ctx := context.Background()

	if got := GetFromContext(ctx); got != nil {
		t.Fatalf("expected no transaction in empty context")
	}

	uow := u.NewTransaction("ctx-1")
	ctx1, err := AddToContext(ctx, uow)
	if err != nil {
		t.Fatalf("unexpected error adding to context: %v", err)
	}

	if got := GetFromContext(ctx1); got == nil {
		t.Fatalf("expected transaction in context")
	}

	if _, err := AddToContext(ctx1, uow); err == nil {
		t.Fatalf("expected error when adding a second transaction to context")
	}

	if err := RollBack(ctx1); err != nil {
		t.Fatalf("unexpected error on RollBack: %v", err)
	}
	if db.metrics.rollbacks == 0 {
		t.Fatalf("expected a rollback to be performed")
	}
}

// transactionError for testing AutoCommit behavior
type transactionError struct{ doCommit bool }

func (e transactionError) Error() string  { return "transaction error" }
func (e transactionError) DoCommit() bool { return e.doCommit }

func TestAutoCommit_NoError_CommitsAll(t *testing.T) {
	db := NewMockDB()
	u := New(db)
	uow := u.NewTransaction("autocommit-ok")
	ctx, err := AddToContext(context.Background(), uow)
	if err != nil {
		t.Fatalf("unexpected error adding to context: %v", err)
	}
	// Create a nested start so CommitAll loops at least once
	uow.Start()

	if err := AutoCommit(ctx, nil); err != nil {
		t.Fatalf("unexpected error from AutoCommit: %v", err)
	}
	if db.metrics.commits != 1 {
		t.Fatalf("expected one commit, got %d", db.metrics.commits)
	}
	if db.metrics.rollbacks != 0 {
		t.Fatalf("did not expect rollback, got %d", db.metrics.rollbacks)
	}
}

func TestAutoCommit_WithNonTransactionError_Rollbacks(t *testing.T) {
	db := NewMockDB()
	u := New(db)
	uow := u.NewTransaction("autocommit-err")
	ctx, err := AddToContext(context.Background(), uow)
	if err != nil {
		t.Fatalf("unexpected error adding to context: %v", err)
	}

	if err := AutoCommit(ctx, errors.New("something failed")); err != nil {
		t.Fatalf("unexpected error from AutoCommit: %v", err)
	}
	if db.metrics.rollbacks != 1 {
		t.Fatalf("expected rollback once, got %d", db.metrics.rollbacks)
	}
	if db.metrics.commits != 0 {
		t.Fatalf("did not expect commit, got %d", db.metrics.commits)
	}
}

func TestAutoCommit_WithTransactionError_Decision(t *testing.T) {
	// DoCommit = true => commit
	{
		db := NewMockDB()
		u := New(db)
		uow := u.NewTransaction("autocommit-commit")
		ctx, _ := AddToContext(context.Background(), uow)
		uow.Start()
		if err := AutoCommit(ctx, transactionError{doCommit: true}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if db.metrics.commits != 1 || db.metrics.rollbacks != 0 {
			t.Fatalf("expected commit once and no rollback, got commits=%d rollbacks=%d", db.metrics.commits, db.metrics.rollbacks)
		}
	}

	// DoCommit = false => rollback
	{
		db := NewMockDB()
		u := New(db)
		uow := u.NewTransaction("autocommit-rollback")
		ctx, _ := AddToContext(context.Background(), uow)
		if err := AutoCommit(ctx, transactionError{doCommit: false}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if db.metrics.rollbacks != 1 || db.metrics.commits != 0 {
			t.Fatalf("expected rollback once and no commit, got commits=%d rollbacks=%d", db.metrics.commits, db.metrics.rollbacks)
		}
	}
}

func TestAutoCommit_ContextForceRollbackOverrides(t *testing.T) {
	db := NewMockDB()
	u := New(db)
	uow := u.NewTransaction("force-on-context")
	ctxWithForce := SetContextCriticalStatusToForceRollback(context.Background())
	ctx, err := AddToContext(ctxWithForce, uow)
	if err != nil {
		t.Fatalf("unexpected error adding to context: %v", err)
	}
	if err := AutoCommit(ctx, nil); err != nil {
		t.Fatalf("unexpected error from AutoCommit: %v", err)
	}
	if db.metrics.rollbacks != 1 {
		t.Fatalf("expected rollback due to forced status, got %d", db.metrics.rollbacks)
	}
	if db.metrics.commits != 0 {
		t.Fatalf("did not expect commit when forced to rollback, got %d", db.metrics.commits)
	}
}

func TestGenerateName_Format(t *testing.T) {
	n := GenerateName(1)
	if !strings.Contains(n, "_") {
		t.Fatalf("expected generated name to contain underscore separator, got %q", n)
	}
	if len(n) == 0 {
		t.Fatalf("expected non-empty name")
	}
}
