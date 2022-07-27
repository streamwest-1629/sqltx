package sqltx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
)

type (
	DB struct {
		lock sync.RWMutex
		db   *sql.DB
	}

	Tx struct {
		parent *DB
		ctx    context.Context
		opts   *sql.TxOptions
		tx     *sql.Tx
	}

	Stmt struct {
		parent *DB
		stmt   *sql.Stmt
	}

	ExecFunc func(tx *Tx) (commit bool)
	txCloser func() error
)

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	} else {
		return &DB{db: db}, nil
	}
}

func OpenDB(c driver.Connector) *DB {
	return &DB{db: sql.OpenDB(c)}
}

func (db *DB) Begin(exec ExecFunc) error {
	return db.BeginTx(context.Background(), nil, exec)
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions, exec ExecFunc) error {
	tx, err := db.db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}

	commit := exec(&Tx{
		tx:     tx,
		ctx:    ctx,
		opts:   opts,
		parent: db,
	})

	db.lock.Lock()
	defer db.lock.Unlock()

	if commit {
		if err := tx.Commit(); err != nil {
			return ErrFinally{
				InternalErr: err,
				Finally:     FinallyCommandCommit,
			}
		}
	} else {
		if err := tx.Rollback(); err != nil {
			return ErrFinally{
				InternalErr: err,
				Finally:     FinallyCommandRollback,
			}
		}
	}

	return nil
}

func (db *DB) Close() error {
	db.lock.Lock()
	defer db.lock.Unlock()
	return db.db.Close()
}

func (tx *Tx) Commit() (closed bool, err error) {
	return tx.remakeTx(tx.tx.Commit)
}

func (tx *Tx) Rollback() (closed bool, err error) {
	return tx.remakeTx(tx.tx.Rollback)
}

func (tx *Tx) remakeTx(closer txCloser) (closed bool, err error) {
	tx.parent.lock.Lock()
	defer tx.parent.lock.Unlock()

	if err := closer(); err != nil {
		return false, err
	} else if renew, err := tx.parent.db.BeginTx(tx.ctx, tx.opts); err != nil {
		return true, err
	} else {
		tx.tx = renew
		return false, nil
	}
}

func (tx *Tx) Prepare(query string) (*Stmt, error) {
	return tx.PrepareContext(context.Background(), query)
}

func (tx *Tx) PrepareContext(ctx context.Context, query string) (*Stmt, error) {
	tx.parent.lock.RLock()
	defer tx.parent.lock.RUnlock()

	if stmt, err := tx.tx.PrepareContext(ctx, query); err != nil {
		return nil, err
	} else {
		return &Stmt{
			parent: tx.parent,
			stmt:   stmt,
		}, nil
	}
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.ExecContext(context.Background(), query, args...)
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	tx.parent.lock.RLock()
	defer tx.parent.lock.RUnlock()
	return tx.tx.ExecContext(ctx, query, args...)
}

func (tx *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return tx.QueryContext(context.Background(), query, args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	tx.parent.lock.RLock()
	defer tx.parent.lock.RUnlock()
	return tx.tx.QueryContext(ctx, query, args...)
}

func (tx *Tx) QueryRow(query string, args ...interface{}) *sql.Row {
	return tx.QueryRowContext(context.Background(), query, args...)
}

func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	tx.parent.lock.RLock()
	defer tx.parent.lock.RUnlock()
	return tx.tx.QueryRowContext(ctx, query, args...)
}

func (stmt *Stmt) Exec(args ...interface{}) (sql.Result, error) {
	return stmt.ExecContext(context.Background(), args...)
}

func (stmt *Stmt) ExecContext(ctx context.Context, args ...interface{}) (sql.Result, error) {
	stmt.parent.lock.RLock()
	defer stmt.parent.lock.RUnlock()
	return stmt.stmt.ExecContext(ctx, args...)
}

func (stmt *Stmt) Query(args ...interface{}) (*sql.Rows, error) {
	return stmt.QueryContext(context.Background(), args...)
}

func (stmt *Stmt) QueryContext(ctx context.Context, args ...interface{}) (*sql.Rows, error) {
	stmt.parent.lock.RLock()
	defer stmt.parent.lock.RUnlock()
	return stmt.stmt.QueryContext(ctx, args...)
}

func (stmt *Stmt) QueryRow(args ...interface{}) *sql.Row {
	return stmt.QueryRowContext(context.Background(), args...)
}

func (stmt *Stmt) QueryRowContext(ctx context.Context, args ...interface{}) *sql.Row {
	stmt.parent.lock.RLock()
	defer stmt.parent.lock.RUnlock()
	return stmt.stmt.QueryRowContext(ctx, args...)
}
