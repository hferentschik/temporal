package gocql

import (
	"context"

	"github.com/gocql/gocql"
)

// Note: this file defines the minimal interface that is needed by Temporal's cassandra
// persistence implementation and should be implemented for all gocql libraries if
// they need to be used.
// Please add more methods to the interface if needed by the cassandra implementation.

type (
	// Session is the interface for interacting with the database.
	Session interface {
		Query(string, ...any) Query
		NewBatch(BatchType) *Batch
		ExecuteBatch(*Batch) error
		MapExecuteBatchCAS(*Batch, map[string]any) (bool, Iter, error)
		AwaitSchemaAgreement(ctx context.Context) error
		Close()
	}

	// GocqlSession is an interface compatible with the concrete type *gocql.Session.
	//
	// It only declares the functions which are actually used in Temporal,
	// so that when declaring a compatible type, only what is strictly required
	// needs to be implemented.
	//
	// This is usefull when using the `CreateSessionFunc` parameter of the
	// cassandra config, in order to override the default behavior.
	GocqlSession interface {
		Query(stmt string, values ...interface{}) *gocql.Query
		NewBatch(typ gocql.BatchType) *gocql.Batch
		ExecuteBatch(batch *gocql.Batch) error
		MapExecuteBatchCAS(batch *gocql.Batch, dest map[string]interface{}) (applied bool, iter *gocql.Iter, err error)
		AwaitSchemaAgreement(ctx context.Context) error
		Close()
	}

	// Query is the interface for query object.
	Query interface {
		Exec() error
		Scan(...any) error
		ScanCAS(...any) (bool, error)
		MapScan(map[string]any) error
		MapScanCAS(map[string]any) (bool, error)
		Iter() Iter
		PageSize(int) Query
		PageState([]byte) Query
		WithContext(context.Context) Query
		WithTimestamp(int64) Query
		Consistency(Consistency) Query
		Bind(...any) Query
		Idempotent(bool) Query
		SetSpeculativeExecutionPolicy(SpeculativeExecutionPolicy) Query
	}

	// Iter is the interface for executing and iterating over all resulting rows.
	Iter interface {
		Scan(...any) bool
		MapScan(map[string]any) bool
		PageState() []byte
		Close() error
	}

	// BatchType is the type of the Batch operation
	BatchType byte

	// Consistency is the consistency level used by a Query
	Consistency uint16

	// SerialConsistency is the serial consistency level used by a Query
	SerialConsistency uint16

	// SpeculativeExecutionPolicy is a gocql SpeculativeExecutionPolicy
	SpeculativeExecutionPolicy gocql.SpeculativeExecutionPolicy
)

var _ GocqlSession = &gocql.Session{}
