## unitow

Lightweight unit-of-work helper for Go. It abstracts transaction lifecycle (begin/commit/rollback), supports nesting with reference counting, integrates with `context.Context`, and provides a convenient `AutoCommit` pattern.

### Features
- Nested transaction `Start`/`Commit` with reference counting
- Context helpers: `AddToContext`, `GetFromContext`, `RollBack`
- `AutoCommit(ctx, err)` to commit on success or rollback on error/panic
- Force rollback via context critical status
- Pluggable `Database` interface (adapter over your DB/ORM)

### Install
Replace github.com/bi0dread/unitow with your repo path if needed.

```bash
go get github.com/bi0dread/unitow
```

### Database adapter
Implement the `Database` interface for your DB/ORM. Example for GORM:

```go
package yourpkg

import (
    "gorm.io/gorm"
    unitow "github.com/bi0dread/unitow"
)

type GormDB struct{ DB *gorm.DB }

func (g GormDB) Begin() unitow.Database   { return GormDB{DB: g.DB.Begin()} }
func (g GormDB) Commit() error            { return g.DB.Commit().Error }
func (g GormDB) Rollback() error          { return g.DB.Rollback().Error }
```

### Quick start
Use `AddToContext` to attach a transaction and `AutoCommit` to handle commit/rollback automatically. Use a named return `err`.

```go
func CreateOrder(ctx context.Context, db *gorm.DB) (err error) {
    u := unitow.New(GormDB{DB: db})
    ctx, err = unitow.AddToContext(ctx, u.NewTransaction("CreateOrder"))
    if err != nil { return err }
    defer func() { _ = unitow.AutoCommit(ctx, err) }()

    // Use transactional DB
    tx := unitow.GetFromContext(ctx).Transaction().(GormDB).DB

    // ... perform writes with tx ...
    return nil
}
```

### Nesting example
`Start`/`Commit` can be nested; commit only happens when the outermost scope commits.

```go
trx := u.NewTransaction("ComplexOperation")
trx.Start()
defer trx.Commit()

// inner
trx.Start()
defer trx.Commit()

// work ...
```

### Force rollback via context
You can force a rollback regardless of success by setting the critical status in context.

```go
ctx = unitow.SetContextCriticalStatusToForceRollback(ctx)
```

### Conditional commit with TransactionError
If your function returns an error that implements `TransactionError`, `AutoCommit` will commit or rollback based on `DoCommit()`.

```go
type txErr struct{ commit bool }
func (e txErr) Error() string   { return "reason" }
func (e txErr) DoCommit() bool  { return e.commit }

func Handler(ctx context.Context) (err error) {
    u := unitow.New(GormDB{DB: db})
    ctx, err = unitow.AddToContext(ctx, u.NewTransaction("Handler"))
    if err != nil { return err }
    defer func() { _ = unitow.AutoCommit(ctx, err) }()

    // ...
    return txErr{commit: true}
}
```

### API surface
- `New(db Database) Unitow`
- `Unitow.NewTransaction(name string) Transaction`
- `Transaction.Start()` / `Transaction.Commit()` / `Transaction.CommitAll()` / `Transaction.Rollback()`
- `Transaction.Transaction() Database` (returns transactional DB if active)
- `AddToContext(ctx, uow)` / `GetFromContext(ctx)` / `RollBack(ctx)`
- `AutoCommit(ctx, err)`
- `SetContextCriticalStatusToForceRollback(ctx)`
- `GenerateName(skip int)`
- `type TransactionError interface { DoCommit() bool }`

### Testing
```bash
go test ./...
```



