// Copyright (c) 2025 ADBC Drivers Contributors
// Copyright (c) 2026 SingleStore, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package singlestore_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/validation"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/singlestore-labs/singlestore-adbc-connector"
)

// SingleStoreQuirks implements validation.DriverQuirks for SingleStore ADBC driver
type SingleStoreQuirks struct {
	dsn string
	mem *memory.CheckedAllocator
}

func (q *SingleStoreQuirks) QuoteTableName(tableName string) string {
	return "`" + strings.ReplaceAll(tableName, "`", "``") + "`"
}

func (q *SingleStoreQuirks) SetupDriver(t *testing.T) driverbase.DriverWithContext {
	q.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	return singlestore.NewDriver(q.mem)
}

func (q *SingleStoreQuirks) TearDownDriver(t *testing.T, _ driverbase.DriverWithContext) {
	q.mem.AssertSize(t, 0)
}

func (q *SingleStoreQuirks) DatabaseOptions() map[string]string {
	return map[string]string{
		adbc.OptionKeyURI: q.dsn,
	}
}

func (q *SingleStoreQuirks) CreateSampleTable(tableName string, r arrow.RecordBatch) error {
	// Use standard database/sql to create table directly
	db, err := sql.Open("mysql", q.dsn)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	// Drop table if it exists first to ensure clean state
	_, err = db.Exec("DROP TABLE IF EXISTS " + tableName)
	if err != nil {
		return fmt.Errorf("failed to drop existing table: %w", err)
	}

	// Build CREATE TABLE statement based on Arrow schema
	var createQuery strings.Builder
	createQuery.WriteString("CREATE TABLE ")
	createQuery.WriteString(tableName)
	createQuery.WriteString(" (")

	schema := r.Schema()
	for i, field := range schema.Fields() {
		if i > 0 {
			createQuery.WriteString(", ")
		}
		createQuery.WriteString(field.Name)
		createQuery.WriteString(" ")

		// Map Arrow types to SingleStore types
		switch field.Type.ID() {
		case arrow.INT32:
			createQuery.WriteString("INT")
		case arrow.INT64:
			createQuery.WriteString("BIGINT")
		case arrow.STRING:
			createQuery.WriteString("VARCHAR(255)")
		case arrow.FLOAT32:
			createQuery.WriteString("FLOAT")
		case arrow.FLOAT64:
			createQuery.WriteString("DOUBLE")
		case arrow.BOOL:
			createQuery.WriteString("BOOLEAN")
		default:
			createQuery.WriteString("TEXT") // Default fallback
		}

		if !field.Nullable {
			createQuery.WriteString(" NOT NULL")
		}
	}
	createQuery.WriteString(")")

	_, err = db.Exec(createQuery.String())
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert data from Arrow record
	if r.NumRows() > 0 {
		// Insert each row separately to handle NULL values correctly
		for row := range r.NumRows() {
			var insertQuery strings.Builder
			insertQuery.WriteString("INSERT INTO ")
			insertQuery.WriteString(tableName)
			insertQuery.WriteString(" VALUES (")

			values := make([]interface{}, r.NumCols())
			for col := range r.NumCols() {
				column := r.Column(int(col))
				if column.IsNull(int(row)) {
					values[col] = nil
				} else {
					// Extract value based on column type
					switch arr := column.(type) {
					case *array.Int32:
						values[col] = arr.Value(int(row))
					case *array.Int64:
						values[col] = arr.Value(int(row))
					case *array.String:
						values[col] = arr.Value(int(row))
					case *array.Float32:
						values[col] = arr.Value(int(row))
					case *array.Float64:
						values[col] = arr.Value(int(row))
					case *array.Boolean:
						values[col] = arr.Value(int(row))
					default:
						values[col] = fmt.Sprintf("%v", column)
					}
				}
			}

			// Build placeholders and collect non-null values for prepared statement
			var queryParams []interface{}
			for i, val := range values {
				if i > 0 {
					insertQuery.WriteString(", ")
				}
				if val == nil {
					insertQuery.WriteString("NULL")
				} else {
					insertQuery.WriteString("?")
					queryParams = append(queryParams, val)
				}
			}
			insertQuery.WriteString(")")

			_, err = db.Exec(insertQuery.String(), queryParams...)
			if err != nil {
				return fmt.Errorf("failed to insert row %d: %w", row, err)
			}
		}
	}

	return nil
}

func (q *SingleStoreQuirks) DropTable(cnxn adbc.ConnectionWithContext, tblName string) error {
	ctx := context.Background()
	stmt, err := cnxn.NewStatement(ctx)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, stmt.Close(ctx))
	}()

	if err = stmt.SetSqlQuery(ctx, "DROP TABLE IF EXISTS "+tblName); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(context.Background())
	return err
}

func (q *SingleStoreQuirks) SampleTableSchemaMetadata(tblName string, dt arrow.DataType) arrow.Metadata {
	// Return metadata that matches what our SingleStore type converter actually returns
	metadata := map[string]string{}

	switch dt.ID() {
	case arrow.INT32:
		metadata["sql.column_name"] = "ints"
		metadata["sql.database_type_name"] = "INT"
		metadata["sql.precision"] = "10"
		metadata["sql.scale"] = "0"
	case arrow.INT64:
		metadata["sql.column_name"] = "ints"
		metadata["sql.database_type_name"] = "BIGINT"
		metadata["sql.precision"] = "19"
		metadata["sql.scale"] = "0"
	case arrow.STRING:
		metadata["sql.column_name"] = "strings"
		metadata["sql.database_type_name"] = "VARCHAR"
		metadata["sql.length"] = "255"
	case arrow.FLOAT32:
		metadata["sql.column_name"] = "floats"
		metadata["sql.database_type_name"] = "FLOAT"
	case arrow.FLOAT64:
		metadata["sql.column_name"] = "doubles"
		metadata["sql.database_type_name"] = "DOUBLE"
	case arrow.BOOL:
		metadata["sql.column_name"] = "bools"
		metadata["sql.database_type_name"] = "TINYINT"
	}

	return arrow.MetadataFrom(metadata)
}

func (q *SingleStoreQuirks) Alloc() memory.Allocator      { return q.mem }
func (q *SingleStoreQuirks) BindParameter(idx int) string { return "?" }

func (q *SingleStoreQuirks) SupportsBulkIngest(string) bool        { return true }
func (q *SingleStoreQuirks) SupportsConcurrentStatements() bool    { return false }
func (q *SingleStoreQuirks) SupportsCurrentCatalogSchema() bool    { return true }
func (q *SingleStoreQuirks) SupportsExecuteSchema() bool           { return true }
func (q *SingleStoreQuirks) SupportsGetSetOptions() bool           { return true }
func (q *SingleStoreQuirks) SupportsPartitionedData() bool         { return false }
func (q *SingleStoreQuirks) SupportsStatistics() bool              { return false }
func (q *SingleStoreQuirks) SupportsTransactions() bool            { return true }
func (q *SingleStoreQuirks) SupportsGetParameterSchema() bool      { return false }
func (q *SingleStoreQuirks) SupportsDynamicParameterBinding() bool { return true }

// TODO: enable when error check is fixed
// https://github.com/adbc-drivers/driverbase-go/pull/123
func (q *SingleStoreQuirks) SupportsErrorIngestIncompatibleSchema() bool { return false }
func (q *SingleStoreQuirks) Catalog() string                             { return "db" }
func (q *SingleStoreQuirks) DBSchema() string                            { return "" }

func (q *SingleStoreQuirks) GetMetadata(code adbc.InfoCode) interface{} {
	switch code {
	case adbc.InfoDriverName:
		return "ADBC Driver for SingleStore"
	case adbc.InfoDriverVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoVendorVersion:
		return os.Getenv("SINGLESTORE_VERSION") + " (SingleStoreDB source distribution (compatible; MySQL Enterprise & MySQL Commercial))"
	case adbc.InfoVendorArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverADBCVersion:
		return adbc.AdbcVersion1_1_0
	case adbc.InfoVendorName:
		return "SingleStore"
	case adbc.InfoVendorSql:
		return true
	case adbc.InfoVendorSubstrait:
		return false
	}
	return nil
}

func withQuirks(t *testing.T, fn func(*SingleStoreQuirks)) {
	dsn := os.Getenv("SINGLESTORE_DSN")
	if dsn == "" {
		t.Skip("Set SINGLESTORE_DSN environment variable for validation tests")
	}

	q := &SingleStoreQuirks{dsn: dsn}
	fn(q)
}

type SingleStoreStatementTests struct {
	validation.StatementTests
}

func (s *SingleStoreStatementTests) TestSqlIngestErrors() {
	s.T().Skip()
}

// TestValidation runs the comprehensive ADBC validation test suite
// This is the primary test that validates ADBC specification compliance
func TestValidation(t *testing.T) {
	withQuirks(t, func(q *SingleStoreQuirks) {
		suite.Run(t, &validation.DatabaseTests{Quirks: q})
		suite.Run(t, &validation.ConnectionTests{Quirks: q})
		suite.Run(t, &validation.StatementTests{Quirks: q})
	})
}

// -------------------- Additional Tests --------------------

type SingleStoreTests struct {
	suite.Suite

	Quirks *SingleStoreQuirks

	ctx    context.Context
	driver driverbase.Driver
	db     adbc.DatabaseWithContext
	cnxn   adbc.ConnectionWithContext
	stmt   adbc.StatementWithContext
}

func (s *SingleStoreTests) SetupTest() {
	var err error
	s.ctx = context.Background()
	s.driver = s.Quirks.SetupDriver(s.T())
	s.db, err = s.driver.NewDatabaseWithContext(s.ctx, s.Quirks.DatabaseOptions())
	s.NoError(err)
	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)
	s.stmt, err = s.cnxn.NewStatement(s.ctx)
	s.NoError(err)
}

func (s *SingleStoreTests) TearDownTest() {
	s.NoError(s.stmt.Close(s.ctx))
	s.NoError(s.cnxn.Close(s.ctx))
	s.Quirks.TearDownDriver(s.T(), s.driver)
	s.cnxn = nil
	s.NoError(s.db.Close(s.ctx))
	s.db = nil
	s.driver = nil
}

type selectCase struct {
	name     string
	query    string
	schema   *arrow.Schema
	expected string
}

func (s *SingleStoreTests) TestBulkInsertTime() {
	s.NoError(s.stmt.SetSqlQuery(s.ctx, "CREATE TEMPORARY TABLE test_bulk_insert_time (id INT PRIMARY KEY, t1 TIME, t2 TIME(6))"))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false,
			Metadata: arrow.MetadataFrom(map[string]string{"sql.column_name": "id", "sql.database_type_name": "INT"}),
		},
		{Name: "t1", Type: arrow.FixedWidthTypes.Time32s, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{"sql.column_name": "t1", "sql.database_type_name": "TIME", "sql.fractional_seconds_precision": "0"}),
		},
		{Name: "t2", Type: arrow.FixedWidthTypes.Time64us, Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{"sql.column_name": "t2", "sql.database_type_name": "TIME", "sql.fractional_seconds_precision": "6"}),
		},
	}, nil)

	batchbldr := array.NewRecordBuilder(s.Quirks.Alloc(), schema)
	defer batchbldr.Release()
	bldr1 := batchbldr.Field(0).(*array.Int32Builder)
	bldr1.AppendValues([]int32{1, 2}, []bool{true, true})

	bldr2 := batchbldr.Field(1).(*array.Time32Builder)
	bldr2.AppendValues([]arrow.Time32{3020399, -3020399}, []bool{true, true})

	bldr3 := batchbldr.Field(2).(*array.Time64Builder)
	bldr3.AppendValues([]arrow.Time64{3020399000000, -3020399000000}, []bool{true, true})

	batch := batchbldr.NewRecordBatch()
	defer batch.Release()

	s.NoError(s.stmt.SetOption(s.ctx, adbc.OptionKeyIngestTargetTable, "test_bulk_insert_time"))
	s.NoError(s.stmt.SetOption(s.ctx, adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend))
	s.NoError(s.stmt.Bind(s.ctx, batch))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	s.NoError(s.stmt.SetSqlQuery(s.ctx, "SELECT * FROM test_bulk_insert_time ORDER BY id"))

	rdr, rows, err := s.stmt.ExecuteQuery(s.ctx)
	s.NoError(err)
	defer rdr.Release()

	s.Equal(int64(-1), rows)
	s.Truef(rdr.Next(), "no record, error? %s", rdr.Err())

	rec := rdr.RecordBatch()
	s.NotNil(rec)

	s.Truef(array.RecordEqual(batch, rec), "expected: %s\ngot: %s", batch, rec)

	s.False(rdr.Next())
	s.NoError(rdr.Err())
}

func (s *SingleStoreTests) TestBulkIngestNull() {
	// Create test table
	s.NoError(s.stmt.SetSqlQuery(s.ctx, `
		CREATE TEMPORARY TABLE test_bulk_ingest_null (
		  id int PRIMARY KEY,
		  c1 LONGTEXT
		)
	`))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "id",
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: false,
		},
		{
			Name:     "c1",
			Type:     arrow.BinaryTypes.StringView,
			Nullable: true,
		},
	}, nil)

	b := array.NewRecordBuilder(s.Quirks.Alloc(), schema)
	defer b.Release()

	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 5, 6, 7}, nil)
	c1Builder := b.Field(1).(*array.StringViewBuilder)
	c1Builder.AppendNull()
	c1Builder.Append("NULL")
	c1Builder.Append("1NULL")
	c1Builder.Append("\n\t")
	c1Builder.Append("\"who is this?\"")
	c1Builder.Append("my \"name\" is")
	c1Builder.Append("\\N")

	rec := b.NewRecordBatch()
	defer rec.Release()

	stream, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	s.NoError(err)
	defer stream.Release()

	s.NoError(s.stmt.SetOption(s.ctx, adbc.OptionKeyIngestTargetTable, "test_bulk_ingest_null"))
	s.NoError(s.stmt.SetOption(s.ctx, adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend))
	s.NoError(s.stmt.BindStream(s.ctx, stream))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	s.NoError(s.stmt.SetSqlQuery(s.ctx, "SELECT id, c1 FROM test_bulk_ingest_null ORDER BY id"))

	rdr, rows, err := s.stmt.ExecuteQuery(s.ctx)
	s.NoError(err)
	defer rdr.Release()

	s.Equal(int64(-1), rows)
	s.Truef(rdr.Next(), "no record, error? %s", rdr.Err())

	out := rdr.RecordBatch()
	s.NotNil(out)
	s.Equal(int64(7), out.NumRows())
	s.Equal(int64(2), out.NumCols())

	idCol := out.Column(0).(*array.Int32)
	s.Equal(int32(1), idCol.Value(0))
	s.Equal(int32(2), idCol.Value(1))
	s.Equal(int32(3), idCol.Value(2))
	s.Equal(int32(4), idCol.Value(3))
	s.Equal(int32(5), idCol.Value(4))
	s.Equal(int32(6), idCol.Value(5))
	s.Equal(int32(7), idCol.Value(6))

	c1Col := out.Column(1).(*array.LargeString)
	s.True(c1Col.IsNull(0))
	s.Equal("NULL", c1Col.Value(1))
	s.Equal("1NULL", c1Col.Value(2))
	s.Equal("\n\t", c1Col.Value(3))
	s.Equal("\"who is this?\"", c1Col.Value(4))
	s.Equal("my \"name\" is", c1Col.Value(5))
	s.Equal("\\N", c1Col.Value(6))

	s.False(rdr.Next())
	s.NoError(rdr.Err())
}

func (s *SingleStoreTests) TestSelect() {
	// Create test table with various SingleStore types including spatial
	s.NoError(s.stmt.SetSqlQuery(s.ctx, `
		CREATE ROWSTORE TEMPORARY TABLE test_types (
		  id int PRIMARY KEY,
		  bool_col BOOL,
		  boolean_col BOOLEAN,
		  bit_col BIT(64),
		  tinyint_col TINYINT,
		  mediumint_col MEDIUMINT,
		  smallint_col SMALLINT,
		  int_col INT,
		  integer_col INTEGER,
		  bigint_col BIGINT,
		  float_col FLOAT,
		  double_col DOUBLE,
		  real_col REAL,
		  date_col DATE,
		  time_col TIME,
		  time6_col TIME(6),
		  datetime_col DATETIME,
		  datetime6_col DATETIME(6),
		  timestamp_col TIMESTAMP,
		  timestamp6_col TIMESTAMP(6),
		  year_col YEAR,
		  decimal_col1 DECIMAL(65, 30),
		  decimal_col2 DECIMAL(38, 10),
		  decimal_col3 DECIMAL(18, 10),
		  decimal_col4 DECIMAL(9, 5),
		  decimal_col5 DECIMAL(18, 0),
		  dec_col DEC(20, 0),
		  fixed_col FIXED(20, 0),
		  numeric_col NUMERIC(20, 0),
		  char_col CHAR,
		  mediumtext_col MEDIUMTEXT,
		  binary_col BINARY,
		  varchar_col VARCHAR(100),
		  varbinary_col VARBINARY(100),
		  longtext_col LONGTEXT,
		  text_col TEXT,
		  tinytext_col TINYTEXT,
		  longblob_col LONGBLOB,
		  mediumblob_col MEDIUMBLOB,
		  blob_col BLOB,
		  tinyblob_col TINYBLOB,
		  json_col JSON,
		  bson_col BSON,
		  enum_col ENUM('active', 'inactive'),
		  set_col SET('a', 'b', 'c'),
		  geography_col GEOGRAPHY,
		  geographypoint_col GEOGRAPHYPOINT,
		  vector_i8_col VECTOR(2, I8),
		  vector_i16_col VECTOR(2, I16),
		  vector_i32_col VECTOR(2, I32),
		  vector_i64_col VECTOR(2, I64),
		  vector_f32_col VECTOR(2, F32),
		  vector_f64_col VECTOR(2, F64)
		)
	`))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	// Insert test data including spatial data
	s.NoError(s.stmt.SetSqlQuery(s.ctx, `
		INSERT INTO test_types VALUES (
		  1,                          -- id
		  0,                          -- bool_col
		  0,                          -- boolean_col
		  0,                          -- bit_col
		  -128,                       -- tinyint_col
		  -8388608,                   -- mediumint_col
		  -32768,                     -- smallint_col
		  -2147483648,                -- int_col
		  -2147483648,                -- integer_col
		  -9223372036854775808,       -- bigint_col
		  -3.402E+38,           -- float_col
		  -1.7976931348623157E+308,   -- double_col
		  -1.7976931348623157E+308,   -- real_col
		  '1000-01-01',               -- date_col
		  '-838:59:59',               -- time_col
		  '-838:59:59.000000',        -- time6_col
		  '1000-01-01 00:00:00',      -- datetime_col
		  '1000-01-01 00:00:00.000000', -- datetime6_col
		  '1970-01-01 00:00:01',      -- timestamp_col
		  '1970-01-01 00:00:01.000000', -- timestamp6_col
		  1901,                       -- year_col
		  '-99999999999999999999999999999999999.999999999999999999999999999999', -- decimal_col1
		  '-9999999999999999999999999999.9999999999', -- decimal_col2
		  '-99999999.9999999999',     -- decimal_col3
		  '-9999.99999',              -- decimal_col4
		  '-999999999999999999',      -- decimal_col5
		  -9999999999,                -- dec_col
		  -9999999999,                -- fixed_col
		  -9999999999,                -- numeric_col
		  '',                         -- char_col
		  '',                         -- mediumtext_col
		  0x00,                       -- binary_col
		  '',                         -- varchar_col
		  0x00,                       -- varbinary_col
		  '',                         -- longtext_col
		  '',                         -- text_col
		  '',                         -- tinytext_col
		  0x00,                       -- longblob_col
		  0x00,                       -- mediumblob_col
		  0x00,                       -- blob_col
		  0x00,                       -- tinyblob_col
		  '{}',                       -- json_col
		  '{}',                       -- bson_col
		  'active',                   -- enum_col
		  '',                         -- set_col
		  NULL,                       -- geography_col
		  NULL,                       -- geographypoint_col
		  '[-128, -128]',         -- vector_i8_col
		  '[-32768, -32768]',     -- vector_i16_col
		  '[-2147483648, -2147483648]', -- vector_i32_col
		  '[-9223372036854775808, -9223372036854775808]', -- vector_i64_col
		  '[-3.402823466E+38, -3.402823466E+38]', -- vector_f32_col
		  '[-1.7976931348623157E+308, -1.7976931348623157E+308]' -- vector_f64_col
		),
		(
		  2,                          -- id
		  1,                          -- bool_col
		  1,                          -- boolean_col
		  18446744073709551615,       -- bit_col
		  127,                        -- tinyint_col
		  8388607,                    -- mediumint_col
		  32767,                      -- smallint_col
		  2147483647,                 -- int_col
		  2147483647,                 -- integer_col
		  9000000000000000000,        -- bigint_col
		  3.402E+38,            -- float_col
		  1.7976931348623157E+308,    -- double_col
		  1.7976931348623157E+308,    -- real_col
		  '9999-12-31',               -- date_col
		  '838:59:59',                -- time_col
		  '838:59:58.999999',         -- time6_col
		  '9999-12-31 23:59:59',      -- datetime_col
		  '9999-12-31 23:59:59.999999', -- datetime6_col
		  '2038-01-19 03:14:07',      -- timestamp_col
		  '2038-01-19 03:14:06.999999', -- timestamp6_col
		  2155,                       -- year_col
		  '99999999999999999999999999999999999.999999999999999999999999999999', -- decimal_col1
		  '9999999999999999999999999999.9999999999', -- decimal_col2
		  '99999999.9999999999',      -- decimal_col3
		  '9999.99999',               -- decimal_col4
		  '999999999999999999',       -- decimal_col5
		  9999999999,                 -- dec_col
		  9999999999,                 -- fixed_col
		  9999999999,                 -- numeric_col
		  'Z',                        -- char_col (single character)
		  'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ', -- mediumtext_col (example long string)
		  0xFF,                       -- binary_col (max byte value)
		  'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ', -- varchar_col (100 chars)
		  0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, -- varbinary_col (100 bytes)
		  'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ', -- longtext_col (example string)
		  'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ', -- text_col (example string)
		  'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ', -- tinytext_col (example string)
		  0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, -- longblob_col (example binary)
		  0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, -- mediumblob_col (example binary)
		  0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, -- blob_col (example binary)
		  0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF, -- tinyblob_col (example binary)
		  '{"key":"value"}',          -- json_col
		  '{"key":"value"}',          -- bson_col
		  'inactive',                 -- enum_col
		  'a,b,c',                    -- set_col
		  'POLYGON((0 0, 0 1, 1 1, 0 0))', -- geographypoint_col
		  'POINT(-74.044514 40.689244)', -- geography_col
		  '[127, 127]',           -- vector_i8_col
		  '[32767, 32767]',       -- vector_i16_col
		  '[2147483647, 2147483647]', -- vector_i32_col
		  '[9223372036854775807, 9223372036854775807]', -- vector_i64_col
		  '[3.402823466E+38, 3.402823466E+38]', -- vector_f32_col
		  '[1.7976931348623157E+308, 1.7976931348623157E+308]' -- vector_f64_col
		)
	`))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	s.NoError(s.stmt.SetSqlQuery(s.ctx, "SET @@SESSION.enable_extended_types_metadata=on"))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	s.NoError(s.stmt.SetSqlQuery(s.ctx, "SET @@SESSION.vector_type_project_format=JSON"))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	jsonType, err := extensions.NewJSONType(arrow.BinaryTypes.LargeString)
	s.NoError(err)

	for _, testCase := range []selectCase{
		{
			name:  "bool",
			query: "SELECT bool_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "bool_col",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "bool_col",
						"sql.database_type_name": "TINYINT",
					}),
				},
			}, nil),
			expected: `[{"bool_col": 0}, {"bool_col": 1}]`,
		},
		{
			name:  "boolean",
			query: "SELECT boolean_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "boolean_col",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "boolean_col",
						"sql.database_type_name": "TINYINT",
					}),
				},
			}, nil),
			expected: `[{"boolean_col": 0}, {"boolean_col": 1}]`,
		},
		{
			name:  "bit",
			query: "SELECT bit_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "bit_col",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "bit_col",
						"sql.database_type_name": "BIT",
					}),
				},
			}, nil),
			expected: `[{"bit_col": "AAAAAAAAAAA="}, {"bit_col": "//////////8="}]`,
		},
		{
			name:  "tinyint",
			query: "SELECT tinyint_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "tinyint_col",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "tinyint_col",
						"sql.database_type_name": "TINYINT",
					}),
				},
			}, nil),
			expected: `[{"tinyint_col": -128}, {"tinyint_col": 127}]`,
		},
		{
			name:  "mediumint",
			query: "SELECT mediumint_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "mediumint_col",
					Type:     arrow.PrimitiveTypes.Int32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "mediumint_col",
						"sql.database_type_name": "MEDIUMINT",
					}),
				},
			}, nil),
			expected: `[{"mediumint_col": -8388608}, {"mediumint_col": 8388607}]`,
		},
		{
			name:  "smallint",
			query: "SELECT smallint_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "smallint_col",
					Type:     arrow.PrimitiveTypes.Int16,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "smallint_col",
						"sql.database_type_name": "SMALLINT",
					}),
				},
			}, nil),
			expected: `[{"smallint_col": -32768}, {"smallint_col": 32767}]`,
		},
		{
			name:  "int",
			query: "SELECT int_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "int_col",
					Type:     arrow.PrimitiveTypes.Int32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "int_col",
						"sql.database_type_name": "INT",
					}),
				},
			}, nil),
			expected: `[{"int_col": -2147483648}, {"int_col": 2147483647}]`,
		},
		{
			name:  "integer",
			query: "SELECT integer_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "integer_col",
					Type:     arrow.PrimitiveTypes.Int32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "integer_col",
						"sql.database_type_name": "INT",
					}),
				},
			}, nil),
			expected: `[{"integer_col": -2147483648}, {"integer_col": 2147483647}]`,
		},
		{
			name:  "bigint",
			query: "SELECT bigint_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "bigint_col",
					Type:     arrow.PrimitiveTypes.Int64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "bigint_col",
						"sql.database_type_name": "BIGINT",
					}),
				},
			}, nil),
			expected: `[{"bigint_col": -9223372036854775808}, {"bigint_col": 9000000000000000000}]`,
		},
		{
			name:  "float",
			query: "SELECT float_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "float_col",
					Type:     arrow.PrimitiveTypes.Float32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "float_col",
						"sql.database_type_name": "FLOAT",
					}),
				},
			}, nil),
			expected: `[{"float_col": -3.402e+38}, {"float_col": 3.402e+38}]`,
		},
		{
			name:  "double",
			query: "SELECT double_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "double_col",
					Type:     arrow.PrimitiveTypes.Float64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "double_col",
						"sql.database_type_name": "DOUBLE",
					}),
				},
			}, nil),
			expected: `[{"double_col": -1.7976931348623157e+308}, {"double_col": 1.7976931348623157e+308}]`,
		},
		{
			name:  "real",
			query: "SELECT real_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "real_col",
					Type:     arrow.PrimitiveTypes.Float64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "real_col",
						"sql.database_type_name": "DOUBLE",
					}),
				},
			}, nil),
			expected: `[{"real_col": -1.7976931348623157e+308}, {"real_col": 1.7976931348623157e+308}]`,
		},
		{
			name:  "decimal1",
			query: "SELECT decimal_col1 FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "decimal_col1",
					Type:     &arrow.Decimal256Type{Precision: 65, Scale: 30},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "decimal_col1",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "65",
						"sql.scale":              "30",
					}),
				},
			}, nil),
			expected: `[{"decimal_col1": "-99999999999999999999999999999999999.999999999999999999999999999999"}, {"decimal_col1": "99999999999999999999999999999999999.999999999999999999999999999999"}]`,
		},
		{
			name:  "decimal2",
			query: "SELECT decimal_col2 FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "decimal_col2",
					Type:     &arrow.Decimal128Type{Precision: 38, Scale: 10},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "decimal_col2",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "38",
						"sql.scale":              "10",
					}),
				},
			}, nil),
			expected: `[{"decimal_col2": "-9999999999999999999999999999.9999999999"}, {"decimal_col2": "9999999999999999999999999999.9999999999"}]`,
		},
		{
			name:  "decimal3",
			query: "SELECT decimal_col3 FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "decimal_col3",
					Type:     &arrow.Decimal64Type{Precision: 18, Scale: 10},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "decimal_col3",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "18",
						"sql.scale":              "10",
					}),
				},
			}, nil),
			expected: `[{"decimal_col3": "-99999999.9999999999"}, {"decimal_col3": "99999999.9999999999"}]`,
		},
		{
			name:  "decimal4",
			query: "SELECT decimal_col4 FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "decimal_col4",
					Type:     &arrow.Decimal32Type{Precision: 9, Scale: 5},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "decimal_col4",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "9",
						"sql.scale":              "5",
					}),
				},
			}, nil),
			expected: `[{"decimal_col4": "-9999.99999"}, {"decimal_col4": "9999.99999"}]`,
		},
		{
			name:  "decimal5",
			query: "SELECT decimal_col5 FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "decimal_col5",
					Type:     &arrow.Decimal64Type{Precision: 18, Scale: 0},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "decimal_col5",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "18",
						"sql.scale":              "0",
					}),
				},
			}, nil),
			expected: `[{"decimal_col5": "-999999999999999999"}, {"decimal_col5": "999999999999999999"}]`,
		},
		{
			name:  "dec",
			query: "SELECT dec_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "dec_col",
					Type:     &arrow.Decimal128Type{Precision: 20, Scale: 0},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "dec_col",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "20",
						"sql.scale":              "0",
					}),
				},
			}, nil),
			expected: `[{"dec_col": -9999999999}, {"dec_col": 9999999999}]`,
		},
		{
			name:  "fixed",
			query: "SELECT fixed_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "fixed_col",
					Type:     &arrow.Decimal128Type{Precision: 20, Scale: 0},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "fixed_col",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "20",
						"sql.scale":              "0",
					}),
				},
			}, nil),
			expected: `[{"fixed_col": -9999999999}, {"fixed_col": 9999999999}]`,
		},
		{
			name:  "numeric",
			query: "SELECT numeric_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "numeric_col",
					Type:     &arrow.Decimal128Type{Precision: 20, Scale: 0},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "numeric_col",
						"sql.database_type_name": "DECIMAL",
						"sql.precision":          "20",
						"sql.scale":              "0",
					}),
				},
			}, nil),
			expected: `[{"numeric_col": -9999999999}, {"numeric_col": 9999999999}]`,
		},
		{
			name:  "date",
			query: "SELECT date_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "date_col",
					Type:     arrow.FixedWidthTypes.Date32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "date_col",
						"sql.database_type_name": "DATE",
					}),
				},
			}, nil),
			expected: `[{"date_col": "1000-01-01"}, {"date_col": "9999-12-31"}]`,
		},
		{
			name:  "time",
			query: "SELECT time_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "time_col",
					Type:     arrow.FixedWidthTypes.Time32s,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":                  "time_col",
						"sql.database_type_name":           "TIME",
						"sql.fractional_seconds_precision": "0",
					}),
				},
			}, nil),
			expected: `[{"time_col": -3020399}, {"time_col": 3020399}]`,
		},
		{
			name:  "time6",
			query: "SELECT time6_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "time6_col",
					Type:     arrow.FixedWidthTypes.Time64us,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":                  "time6_col",
						"sql.database_type_name":           "TIME",
						"sql.fractional_seconds_precision": "6",
					}),
				},
			}, nil),
			expected: `[{"time6_col": -3020399000000}, {"time6_col": 3020398999999}]`,
		},
		{
			name:  "datetime",
			query: "SELECT datetime_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "datetime_col",
					Type:     &arrow.TimestampType{Unit: arrow.Second},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":                  "datetime_col",
						"sql.database_type_name":           "DATETIME",
						"sql.fractional_seconds_precision": "0",
					}),
				},
			}, nil),
			expected: `[{"datetime_col": "1000-01-01 00:00:00"}, {"datetime_col": "9999-12-31 23:59:59"}]`,
		},
		{
			name:  "datetime6",
			query: "SELECT datetime6_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "datetime6_col",
					Type:     &arrow.TimestampType{Unit: arrow.Microsecond},
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":                  "datetime6_col",
						"sql.database_type_name":           "DATETIME",
						"sql.fractional_seconds_precision": "6",
					}),
				},
			}, nil),
			expected: `[{"datetime6_col": "1000-01-01 00:00:00.000000"}, {"datetime6_col": "9999-12-31 23:59:59.999999"}]`,
		},
		{
			name:  "timestamp",
			query: "SELECT timestamp_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "timestamp_col",
					Type:     arrow.FixedWidthTypes.Timestamp_s,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":                  "timestamp_col",
						"sql.database_type_name":           "TIMESTAMP",
						"sql.fractional_seconds_precision": "0",
					}),
				},
			}, nil),
			expected: `[{"timestamp_col": "1970-01-01 00:00:01"}, {"timestamp_col": "2038-01-19 03:14:07"}]`,
		},
		{
			name:  "timestamp6",
			query: "SELECT timestamp6_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "timestamp6_col",
					Type:     arrow.FixedWidthTypes.Timestamp_us,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":                  "timestamp6_col",
						"sql.database_type_name":           "TIMESTAMP",
						"sql.fractional_seconds_precision": "6",
					}),
				},
			}, nil),
			expected: `[{"timestamp6_col": "1970-01-01 00:00:01"}, {"timestamp6_col": "2038-01-19 03:14:06.999999"}]`,
		},
		{
			name:  "year",
			query: "SELECT year_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "year_col",
					Type:     arrow.PrimitiveTypes.Int16,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "year_col",
						"sql.database_type_name": "YEAR",
					}),
				},
			}, nil),
			expected: `[{"year_col": 1901}, {"year_col": 2155}]`,
		},
		{
			name:  "char",
			query: "SELECT char_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "char_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "char_col",
						"sql.database_type_name": "CHAR",
					}),
				},
			}, nil),
			expected: `[{"char_col": ""}, {"char_col": "Z"}]`,
		},
		{
			name:  "mediumtext",
			query: "SELECT mediumtext_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "mediumtext_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "mediumtext_col",
						"sql.database_type_name": "MEDIUMTEXT",
					}),
				},
			}, nil),
			expected: `[{"mediumtext_col": ""}, {"mediumtext_col": "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"}]`,
		},
		{
			name:  "binary",
			query: "SELECT binary_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "binary_col",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "binary_col",
						"sql.database_type_name": "BINARY",
					}),
				},
			}, nil),
			expected: `[{"binary_col": "AA=="}, {"binary_col": "/w=="}]`,
		},
		{
			name:  "varchar",
			query: "SELECT varchar_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "varchar_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "varchar_col",
						"sql.database_type_name": "VARCHAR",
					}),
				},
			}, nil),
			expected: `[{"varchar_col": ""}, {"varchar_col": "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"}]`,
		},
		{
			name:  "varbinary",
			query: "SELECT varbinary_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "varbinary_col",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "varbinary_col",
						"sql.database_type_name": "VARBINARY",
					}),
				},
			}, nil),
			expected: `[{"varbinary_col": "AA=="}, {"varbinary_col": "//////////////////////////////////////////////////////////////////8="}]`,
		},
		{
			name:  "longtext",
			query: "SELECT longtext_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "longtext_col",
					Type:     arrow.BinaryTypes.LargeString,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "longtext_col",
						"sql.database_type_name": "LONGTEXT",
					}),
				},
			}, nil),
			expected: `[{"longtext_col": ""}, {"longtext_col": "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"}]`,
		},
		{
			name:  "text",
			query: "SELECT text_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "text_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "text_col",
						"sql.database_type_name": "TEXT",
					}),
				},
			}, nil),
			expected: `[{"text_col": ""}, {"text_col": "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"}]`,
		},
		{
			name:  "tinytext",
			query: "SELECT tinytext_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "tinytext_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "tinytext_col",
						"sql.database_type_name": "TINYTEXT",
					}),
				},
			}, nil),
			expected: `[{"tinytext_col": ""}, {"tinytext_col": "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"}]`,
		},
		{
			name:  "longblob",
			query: "SELECT longblob_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "longblob_col",
					Type:     arrow.BinaryTypes.LargeBinary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "longblob_col",
						"sql.database_type_name": "LONGBLOB",
					}),
				},
			}, nil),
			expected: `[{"longblob_col": "AA=="}, {"longblob_col": "//////////////////////////////////////////////////////////////////8="}]`,
		},
		{
			name:  "mediumblob",
			query: "SELECT mediumblob_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "mediumblob_col",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "mediumblob_col",
						"sql.database_type_name": "MEDIUMBLOB",
					}),
				},
			}, nil),
			expected: `[{"mediumblob_col": "AA=="}, {"mediumblob_col": "//////////////////////////////////////////////////////////////////8="}]`,
		},
		{
			name:  "blob",
			query: "SELECT blob_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "blob_col",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "blob_col",
						"sql.database_type_name": "BLOB",
					}),
				},
			}, nil),
			expected: `[{"blob_col": "AA=="}, {"blob_col": "//////////////////////////////////////////////////////////////////8="}]`,
		},
		{
			name:  "tinyblob",
			query: "SELECT tinyblob_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "tinyblob_col",
					Type:     arrow.BinaryTypes.Binary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "tinyblob_col",
						"sql.database_type_name": "TINYBLOB",
					}),
				},
			}, nil),
			expected: `[{"tinyblob_col": "AA=="}, {"tinyblob_col": "//////////////////////////////////////////////////////////////////8="}]`,
		},
		{
			name:  "json",
			query: "SELECT json_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "json_col",
					Type:     jsonType,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "json_col",
						"sql.database_type_name": "JSON",
					}),
				},
			}, nil),
			expected: `[{"json_col": "{}"}, {"json_col": "{\"key\":\"value\"}"}]`,
		},
		{
			name:  "bson",
			query: "SELECT bson_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "bson_col",
					Type:     arrow.BinaryTypes.LargeBinary,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "bson_col",
						"sql.database_type_name": "LONGBLOB",
					}),
				},
			}, nil),
			expected: `[{"bson_col": "BQAAAAA="}, {"bson_col": "FAAAAAJrZXkABgAAAHZhbHVlAAA="}]`,
		},
		{
			name:  "enum",
			query: "SELECT enum_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "enum_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "enum_col",
						"sql.database_type_name": "VARCHAR",
					}),
				},
			}, nil),
			expected: `[{"enum_col": "active"}, {"enum_col": "inactive"}]`,
		},
		{
			name:  "set",
			query: "SELECT set_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "set_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "set_col",
						"sql.database_type_name": "VARCHAR",
					}),
				},
			}, nil),
			expected: `[{"set_col": ""}, {"set_col": "a,b,c"}]`,
		},
		{
			name:  "geography",
			query: "SELECT geography_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "geography_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "geography_col",
						"sql.database_type_name": "CHAR",
					}),
				},
			}, nil),
			expected: `[{"geography_col": null}, {"geography_col": "POLYGON((1.00000000 1.00000000, 0.00000000 1.00000000, 0.00000000 0.00000000, 1.00000000 1.00000000))"}]`,
		},
		{
			name:  "geographypoint",
			query: "SELECT geographypoint_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "geographypoint_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "geographypoint_col",
						"sql.database_type_name": "CHAR",
					}),
				},
			}, nil),
			expected: `[{"geographypoint_col": null}, {"geographypoint_col": "POINT(-74.04451396 40.68924403)"}]`,
		},
		{
			name:  "vector_i8",
			query: "SELECT vector_i8_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "vector_i8_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "vector_i8_col",
						"sql.database_type_name": "VARCHAR",
					}),
				},
			}, nil),
			expected: `[{"vector_i8_col": "[-128,-128]"}, {"vector_i8_col": "[127,127]"}]`,
		},
		{
			name:  "vector_i16",
			query: "SELECT vector_i16_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "vector_i16_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "vector_i16_col",
						"sql.database_type_name": "VARCHAR",
					}),
				},
			}, nil),
			expected: `[{"vector_i16_col": "[-32768,-32768]"}, {"vector_i16_col": "[32767,32767]"}]`,
		},
		{
			name:  "vector_i32",
			query: "SELECT vector_i32_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "vector_i32_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "vector_i32_col",
						"sql.database_type_name": "VARCHAR",
					}),
				},
			}, nil),
			expected: `[{"vector_i32_col": "[-2147483648,-2147483648]"}, {"vector_i32_col": "[2147483647,2147483647]"}]`,
		},
		{
			name:  "vector_i64",
			query: "SELECT vector_i64_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "vector_i64_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "vector_i64_col",
						"sql.database_type_name": "VARCHAR",
					}),
				},
			}, nil),
			expected: `[{"vector_i64_col": "[-9223372036854775808,-9223372036854775808]"}, {"vector_i64_col": "[9223372036854775807,9223372036854775807]"}]`,
		},
		{
			name:  "vector_f32",
			query: "SELECT vector_f32_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "vector_f32_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "vector_f32_col",
						"sql.database_type_name": "VARCHAR",
					}),
				},
			}, nil),
			expected: `[{"vector_f32_col": "[-3.40282347e+38,-3.40282347e+38]"}, {"vector_f32_col": "[3.40282347e+38,3.40282347e+38]"}]`,
		},
		{
			name:  "vector_f64",
			query: "SELECT vector_f64_col FROM test_types ORDER BY id",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "vector_f64_col",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "vector_f64_col",
						"sql.database_type_name": "VARCHAR",
					}),
				},
			}, nil),
			expected: `[{"vector_f64_col": "[-1.7976931348623157e+308,-1.7976931348623157e+308]"}, {"vector_f64_col": "[1.7976931348623157e+308,1.7976931348623157e+308]"}]`,
		},
	} {
		s.Run(testCase.name, func() {
			s.NoError(s.stmt.SetSqlQuery(s.ctx, testCase.query))

			rdr, rows, err := s.stmt.ExecuteQuery(s.ctx)
			s.NoError(err)
			defer rdr.Release()

			s.Truef(testCase.schema.Equal(rdr.Schema()), "expected: %s\ngot: %s", testCase.schema, rdr.Schema())
			s.Equal(int64(-1), rows)
			s.Truef(rdr.Next(), "no record, error? %s", rdr.Err())

			expectedRecord, _, err := array.RecordFromJSON(s.Quirks.Alloc(), testCase.schema, bytes.NewReader([]byte(testCase.expected)))
			s.NoError(err)
			defer expectedRecord.Release()

			rec := rdr.RecordBatch()
			s.NotNil(rec)

			s.Truef(array.RecordEqual(expectedRecord, rec), "expected: %s\ngot: %s", expectedRecord, rec)

			s.False(rdr.Next())
			s.NoError(rdr.Err())
		})
	}
}

type SingleStoreTestSuite struct {
	suite.Suite
	dsn    string
	mem    *memory.CheckedAllocator
	ctx    context.Context
	driver driverbase.Driver
	db     adbc.DatabaseWithContext
	cnxn   adbc.ConnectionWithContext
	stmt   adbc.StatementWithContext
}

func (s *SingleStoreTestSuite) SetupSuite() {
	var err error
	s.dsn = os.Getenv("SINGLESTORE_DSN")
	if s.dsn == "" {
		s.T().Skip("Set SINGLESTORE_DSN environment variable")
	}

	s.ctx = context.Background()
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)

	s.driver = singlestore.NewDriver(s.mem)
	s.db, err = s.driver.NewDatabaseWithContext(s.ctx, map[string]string{
		adbc.OptionKeyURI: s.dsn,
	})
	s.NoError(err)

	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)

	s.stmt, err = s.cnxn.NewStatement(s.ctx)
	s.NoError(err)
}

func (s *SingleStoreTestSuite) TearDownSuite() {
	if s.stmt != nil {
		s.NoError(s.stmt.Close(s.ctx))
	}
	if s.cnxn != nil {
		s.NoError(s.cnxn.Close(s.ctx))
	}
	if s.db != nil {
		s.NoError(s.db.Close(s.ctx))
	}
	s.mem.AssertSize(s.T(), 0)
}

func TestSingleStoreTypeTests(t *testing.T) {
	dsn := os.Getenv("SINGLESTORE_DSN")
	if dsn == "" {
		t.Skip("Set SINGLESTORE_DSN environment variable for type tests")
	}

	quirks := &SingleStoreQuirks{dsn: dsn}
	suite.Run(t, &SingleStoreTests{Quirks: quirks})
}

func TestSingleStoreIntegrationSuite(t *testing.T) {
	suite.Run(t, new(SingleStoreTestSuite))
}

func TestInvalidURIDoesNotExposeSecrets(t *testing.T) {
	const password = "uri-password-sentinel"
	const token = "uri-token-sentinel"
	for _, tc := range []struct{ name, uri, diagnostic string }{
		{"escape", "mysql://user:" + password + "@localhost/db%zz?token=" + token, "invalid URL escape"},
		{"port", "mysql://user:" + password + "@localhost:invalid/db?token=" + token, "invalid port"},
		{"hostname", "mysql://user:" + password + "@/db?token=" + token, "missing hostname in URI"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			drv := singlestore.NewDriver(memory.DefaultAllocator)
			db, err := drv.NewDatabaseWithContext(context.Background(), map[string]string{adbc.OptionKeyURI: tc.uri})
			if db != nil {
				defer func() { require.NoError(t, db.Close(context.Background())) }()
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.diagnostic)
			assert.NotContains(t, err.Error(), password)
			assert.NotContains(t, err.Error(), token)
			assert.NotContains(t, err.Error(), tc.uri)
		})
	}
}

// TestURIParsing tests the parseToSingleStoreDSN function with various URI formats
func TestURIParsing(t *testing.T) {
	factory := singlestore.NewSingleStoreDBFactory()

	tests := []struct {
		name           string
		singlestoreURI string
		username       string
		password       string
		autocommit     string
		expectedDSN    string
		shouldError    bool
		errorContains  string
	}{
		// autocommit
		{
			name:           "autocommit is enabled",
			autocommit:     "true",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb?autocommit=true",
		},
		{
			name:           "autocommit is disabled",
			autocommit:     "false",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb?autocommit=false",
		},
		{
			name:           "autocommit is enabled and additional parameters are provided",
			autocommit:     "true",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb?charset=utf8",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb?autocommit=true&charset=utf8",
		},
		// TCP connection variations
		{
			name:           "basic tcp with port",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb",
		},
		{
			name:           "tcp without port - should default to 3306",
			singlestoreURI: "mysql://user:pass@localhost/testdb",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb",
		},
		{
			name:           "tcp without host - should be invalid",
			singlestoreURI: "mysql://user:pass@/testdb",
			shouldError:    true,
			errorContains:  "missing hostname in URI",
		},
		{
			name:           "tcp without database",
			singlestoreURI: "mysql://user:pass@localhost:3306",
			expectedDSN:    "user:pass@tcp(localhost:3306)/",
		},
		{
			name:           "tcp without database but with slash",
			singlestoreURI: "mysql://user:pass@localhost:3306/",
			expectedDSN:    "user:pass@tcp(localhost:3306)/",
		},
		{
			name:           "tcp with custom port",
			singlestoreURI: "mysql://user:pass@example.com:3307/myapp",
			expectedDSN:    "user:pass@tcp(example.com:3307)/myapp",
		},
		{
			name:           "tcp with ip address",
			singlestoreURI: "mysql://user:pass@127.0.0.1:3306/testdb",
			expectedDSN:    "user:pass@tcp(127.0.0.1:3306)/testdb",
		},
		{
			name:           "tcp with ipv6 host",
			singlestoreURI: "mysql://user:pass@[::1]:3306/testdb",
			expectedDSN:    "user:pass@tcp([::1]:3306)/testdb",
		},
		{
			name:           "tcp with ipv6 host, default port",
			singlestoreURI: "mysql://user:pass@[::1]/testdb",
			expectedDSN:    "user:pass@tcp([::1]:3306)/testdb",
		},

		// Credential handling variations
		{
			name:           "no credentials in uri",
			singlestoreURI: "mysql://localhost:3306/testdb",
			expectedDSN:    "tcp(localhost:3306)/testdb",
		},
		{
			name:           "only username in uri",
			singlestoreURI: "mysql://user@localhost:3306/testdb",
			expectedDSN:    "user@tcp(localhost:3306)/testdb",
		},
		{
			name:           "override credentials with options",
			singlestoreURI: "mysql://olduser:oldpass@localhost:3306/testdb",
			username:       "newuser",
			password:       "newpass",
			expectedDSN:    "newuser:newpass@tcp(localhost:3306)/testdb",
		},
		{
			name:           "add credentials via options",
			singlestoreURI: "mysql://localhost:3306/testdb",
			username:       "admin",
			password:       "secret",
			expectedDSN:    "admin:secret@tcp(localhost:3306)/testdb",
		},
		{
			name:           "override only username",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb",
			username:       "newuser",
			expectedDSN:    "newuser:pass@tcp(localhost:3306)/testdb",
		},
		{
			name:           "override only password",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb",
			password:       "newpass",
			expectedDSN:    "user:newpass@tcp(localhost:3306)/testdb",
		},

		// Query parameter variations
		{
			name:           "single query parameter",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb?charset=utf8mb4",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb?charset=utf8mb4",
		},
		{
			name:           "multiple query parameters",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb?charset=utf8mb4&timeout=30s&tls=false",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb?charset=utf8mb4&timeout=30s&tls=false",
		},
		{
			name:           "ssl parameters",
			singlestoreURI: "mysql://user:pass@localhost:3306/testdb?tls=skip-verify&timeout=10s",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb?tls=skip-verify&timeout=10s",
		},
		{
			name:           "url encoded database name",
			singlestoreURI: "mysql://user:pass@localhost:3306/test%20db?charset=utf8",
			expectedDSN:    "user:pass@tcp(localhost:3306)/test%20db?charset=utf8",
		},
		{
			name:           "query parameters with encoding",
			singlestoreURI: "mysql://user:pass@localhost/testdb?time_zone=%27%2B00%3A00%27",
			expectedDSN:    "user:pass@tcp(localhost:3306)/testdb?time_zone=%27%2B00%3A00%27",
		},

		// Unix socket variations
		{
			name:           "unix socket with parentheses",
			singlestoreURI: "mysql://user:pass@(/tmp/singlestore.sock)/testdb",
			expectedDSN:    "user:pass@unix(/tmp/singlestore.sock)/testdb",
		},
		{
			name:           "unix socket with percent encoding - should be invalid. Must use parenthesis",
			singlestoreURI: "mysql://user:pass@/tmp%2Fsinglestore.sock/testdb",
			shouldError:    true,
			errorContains:  "missing hostname in URI",
		},
		{
			name:           "unix socket with complex path",
			singlestoreURI: "mysql://user:pass@(/var/run/mysqld/mysqld.sock)/myapp",
			expectedDSN:    "user:pass@unix(/var/run/mysqld/mysqld.sock)/myapp",
		},
		{
			name:           "unix socket without database",
			singlestoreURI: "mysql://user:pass@(/tmp/singlestore.sock)",
			expectedDSN:    "user:pass@unix(/tmp/singlestore.sock)/",
		},
		{
			name:           "unix socket with query params",
			singlestoreURI: "mysql://user:pass@(/tmp/singlestore.sock)/testdb?charset=utf8mb4",
			expectedDSN:    "user:pass@unix(/tmp/singlestore.sock)/testdb?charset=utf8mb4",
		},
		{
			name:           "unix socket with empty host (ambiguous) - should be invalid",
			singlestoreURI: "mysql://user:pass@/tmp/singlestore.sock/testdb",
			shouldError:    true,
			errorContains:  "missing hostname in URI",
		},
		{
			name:           "invalid unix socket (missing parenthesis)",
			singlestoreURI: "mysql://user@(/tmp/singlestore.sock/testdb",
			shouldError:    true,
			errorContains:  "missing closing ')'",
		},
		{
			name:           "unix socket (paren) with encoded db name",
			singlestoreURI: "mysql://user:pass@(/tmp/singlestore.sock)/my%20db?foo=bar",
			expectedDSN:    "user:pass@unix(/tmp/singlestore.sock)/my%20db?foo=bar",
		},
		// Special characters and edge cases
		{
			name:           "credentials with special characters",
			singlestoreURI: "mysql://my%40user:p%40ss%24word@localhost:3306/testdb",
			expectedDSN:    "my@user:p@ss$word@tcp(localhost:3306)/testdb",
		},

		// Error cases
		{
			name:           "invalid singlestore uri format",
			singlestoreURI: "mysql://[invalid-uri",
			shouldError:    true,
			errorContains:  "invalid SingleStore URI format",
		},
		{
			name:           "invalid socket path encoding",
			singlestoreURI: "mysql://user:pass@%ZZ%invalid/testdb",
			shouldError:    true,
			errorContains:  "invalid SingleStore URI format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := map[string]string{
				adbc.OptionKeyURI: tt.singlestoreURI,
			}
			if tt.username != "" {
				opts[adbc.OptionKeyUsername] = tt.username
			}
			if tt.password != "" {
				opts[adbc.OptionKeyPassword] = tt.password
			}
			if tt.autocommit != "" {
				opts[adbc.OptionKeyAutoCommit] = tt.autocommit
			}

			result, err := factory.BuildSingleStoreDSN(opts)

			if tt.shouldError {
				require.ErrorContains(t, err, tt.errorContains)
				return
			}

			require.NoError(t, err, "unexpected error")
			assert.Equal(t, tt.expectedDSN, result, "DSN should match expected value")
		})
	}
}

func createTempCertAndKey(t *testing.T) (string, string) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"SingleStore Test"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	require.NoError(t, err)

	certOut, err := os.CreateTemp("", "test_cert_*.pem")
	require.NoError(t, err)
	defer func() { _ = certOut.Close() }()
	err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	require.NoError(t, err)

	keyBytes, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	keyOut, err := os.CreateTemp("", "test_key_*.pem")
	require.NoError(t, err)
	defer func() { _ = keyOut.Close() }()
	err = pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
	require.NoError(t, err)

	return certOut.Name(), keyOut.Name()
}

func TestCustomTLS(t *testing.T) {
	factory := singlestore.NewSingleStoreDBFactory()
	certFile, keyFile := createTempCertAndKey(t)
	defer func() { _ = os.Remove(certFile) }()
	defer func() { _ = os.Remove(keyFile) }()

	t.Run("valid_ca_in_uri", func(t *testing.T) {
		opts := map[string]string{
			adbc.OptionKeyURI: fmt.Sprintf("mysql://user:pass@localhost:3306/testdb?ssl_ca=%s&tls=verify_ca", url.QueryEscape(certFile)),
		}
		dsn, err := factory.BuildSingleStoreDSN(opts)
		require.NoError(t, err)
		assert.Contains(t, dsn, "tls=custom_ca_")
		assert.NotContains(t, dsn, "ssl_ca=")
	})

	t.Run("valid_ca_in_opts", func(t *testing.T) {
		opts := map[string]string{
			adbc.OptionKeyURI: "mysql://user:pass@localhost:3306/testdb",
			"ssl_ca":          certFile,
			"ssl_mode":        "verify_ca",
		}
		dsn, err := factory.BuildSingleStoreDSN(opts)
		require.NoError(t, err)
		assert.Contains(t, dsn, "tls=custom_ca_")
		assert.NotContains(t, dsn, "ssl_ca=")
	})

	t.Run("valid_client_cert_and_key", func(t *testing.T) {
		opts := map[string]string{
			adbc.OptionKeyURI: "mysql://user:pass@localhost:3306/testdb",
			"ssl_ca":          certFile,
			"ssl_cert":        certFile,
			"ssl_key":         keyFile,
		}
		dsn, err := factory.BuildSingleStoreDSN(opts)
		require.NoError(t, err)
		assert.Contains(t, dsn, "tls=custom_ca_")
	})

	t.Run("missing_ca_file_errors", func(t *testing.T) {
		opts := map[string]string{
			adbc.OptionKeyURI: "mysql://user:pass@localhost:3306/testdb?ssl_ca=/nonexistent/path/ca.pem",
		}
		_, err := factory.BuildSingleStoreDSN(opts)
		require.ErrorContains(t, err, "failed to read ssl_ca file")
	})

	t.Run("invalid_ca_pem_errors", func(t *testing.T) {
		badFile, err := os.CreateTemp("", "bad_ca_*.pem")
		require.NoError(t, err)
		defer func() { _ = os.Remove(badFile.Name()) }()
		_, _ = badFile.WriteString("this is not a valid pem certificate")
		require.NoError(t, badFile.Close())

		opts := map[string]string{
			adbc.OptionKeyURI: "mysql://user:pass@localhost:3306/testdb?ssl_ca=" + badFile.Name(),
		}
		_, err = factory.BuildSingleStoreDSN(opts)
		require.ErrorContains(t, err, "failed to parse root certificates")
	})

	t.Run("cert_without_key_errors", func(t *testing.T) {
		opts := map[string]string{
			adbc.OptionKeyURI: "mysql://user:pass@localhost:3306/testdb?ssl_cert=" + certFile,
		}
		_, err := factory.BuildSingleStoreDSN(opts)
		require.ErrorContains(t, err, "both ssl_cert and ssl_key must be provided")
	})

	t.Run("ssl_ca_with_disabled_mode_errors", func(t *testing.T) {
		opts := map[string]string{
			adbc.OptionKeyURI: fmt.Sprintf("mysql://user:pass@localhost:3306/testdb?ssl_ca=%s&tls=false", url.QueryEscape(certFile)),
		}
		_, err := factory.BuildSingleStoreDSN(opts)
		require.ErrorContains(t, err, `custom TLS options (ssl_ca, ssl_cert, ssl_key) cannot be used when TLS is disabled (tls="false")`)
	})

	t.Run("postgres_style_sslmode_aliases", func(t *testing.T) {
		_, tlsVerifyCA, err := singlestore.BuildCustomTLSConfig("other.example.com", "verify-ca", certFile, "", "")
		require.NoError(t, err)
		assert.True(t, tlsVerifyCA.InsecureSkipVerify, "verify-ca should skip standard hostname check and use VerifyPeerCertificate")
		assert.NotNil(t, tlsVerifyCA.VerifyPeerCertificate, "verify-ca must set VerifyPeerCertificate for CA-only verification")

		_, tlsRequire, err := singlestore.BuildCustomTLSConfig("other.example.com", "require", certFile, "", "")
		require.NoError(t, err)
		assert.True(t, tlsRequire.InsecureSkipVerify, "require should encrypt without verifying peer certificate or hostname")
		assert.Nil(t, tlsRequire.VerifyPeerCertificate, "require must not install VerifyPeerCertificate")
	})

	t.Run("timeout_normalized_with_unit", func(t *testing.T) {
		opts := map[string]string{
			adbc.OptionKeyURI: "mysql://user:pass@localhost:3306/testdb?timeout=15",
		}
		dsn, err := factory.BuildSingleStoreDSN(opts)
		require.NoError(t, err)
		assert.Contains(t, dsn, "timeout=15s")
	})

	t.Run("native_dsn_tls_strips_port_from_server_name", func(t *testing.T) {
		uriDSN, err := factory.BuildSingleStoreDSN(map[string]string{
			adbc.OptionKeyURI: "mysql://user:pass@myhost.example.com:3306/testdb",
			"ssl_ca":          certFile,
			"ssl_mode":        "verify_full",
		})
		require.NoError(t, err)

		nativeDSN, err := factory.BuildSingleStoreDSN(map[string]string{
			adbc.OptionKeyURI: "user:pass@tcp(myhost.example.com:3306)/testdb",
			"ssl_ca":          certFile,
			"ssl_mode":        "verify_full",
		})
		require.NoError(t, err)
		assert.Equal(t, uriDSN, nativeDSN, "URI and native DSN must register the same custom_ca key without port")

		_, tlsCfg, err := singlestore.BuildCustomTLSConfig("myhost.example.com:3306", "verify_full", certFile, "", "")
		require.NoError(t, err)
		assert.Equal(t, "myhost.example.com", tlsCfg.ServerName)
		assert.False(t, tlsCfg.InsecureSkipVerify)
	})

	t.Run("native_dsn_standalone_timeout_options", func(t *testing.T) {
		dsn, err := factory.BuildSingleStoreDSN(map[string]string{
			adbc.OptionKeyURI: "user:pass@tcp(localhost:3306)/testdb",
			"timeout":         "15",
		})
		require.NoError(t, err)
		assert.Contains(t, dsn, "timeout=15s")

		dsnConnect, err := factory.BuildSingleStoreDSN(map[string]string{
			adbc.OptionKeyURI: "user:pass@tcp(localhost:3306)/testdb",
			"connect_timeout": "20s",
		})
		require.NoError(t, err)
		assert.Contains(t, dsnConnect, "timeout=20s")
	})

	t.Run("verify_ca_without_custom_ca_still_verifies_server_cert", func(t *testing.T) {
		_, tlsCfg, err := singlestore.BuildCustomTLSConfig("localhost", "verify_ca", "", certFile, keyFile)
		require.NoError(t, err)
		require.NotNil(t, tlsCfg.VerifyPeerCertificate, "verify_ca must always install VerifyPeerCertificate even without ssl_ca")

		pemBytes, err := os.ReadFile(certFile)
		require.NoError(t, err)
		block, _ := pem.Decode(pemBytes)
		require.NotNil(t, block)

		// Self-signed cert is not in the system root pool, so VerifyPeerCertificate must reject it
		err = tlsCfg.VerifyPeerCertificate([][]byte{block.Bytes}, nil)
		require.Error(t, err)

		// When ssl_ca is provided, the same cert is trusted even with a mismatched hostname
		_, tlsCfgWithCA, err := singlestore.BuildCustomTLSConfig("other.example.com", "verify_ca", certFile, certFile, keyFile)
		require.NoError(t, err)
		require.NotNil(t, tlsCfgWithCA.VerifyPeerCertificate)
		require.NoError(t, tlsCfgWithCA.VerifyPeerCertificate([][]byte{block.Bytes}, nil))
	})
}

func TestSingleStoreHardening(t *testing.T) {
	withQuirks(t, func(q *SingleStoreQuirks) {
		ctx := context.Background()
		drv := q.SetupDriver(t)
		defer q.TearDownDriver(t, drv)

		db, err := drv.NewDatabaseWithContext(ctx, q.DatabaseOptions())
		require.NoError(t, err)
		defer func() { _ = db.Close(ctx) }()

		conn, err := db.Open(ctx)
		require.NoError(t, err)
		defer func() { _ = conn.Close(ctx) }()

		stmt, err := conn.NewStatement(ctx)
		require.NoError(t, err)
		defer func() { _ = stmt.Close(ctx) }()

		t.Run("unsigned_integers", func(t *testing.T) {
			rawDB, err := sql.Open("mysql", q.dsn)
			require.NoError(t, err)
			defer func() { _ = rawDB.Close() }()

			_, _ = rawDB.Exec("DROP TABLE IF EXISTS test_uints")
			_, err = rawDB.Exec("CREATE TABLE test_uints (u8 TINYINT UNSIGNED, u16 SMALLINT UNSIGNED, u32 INT UNSIGNED, u64 BIGINT UNSIGNED)")
			require.NoError(t, err)
			defer func() { _, _ = rawDB.Exec("DROP TABLE IF EXISTS test_uints") }()

			_, err = rawDB.Exec("INSERT INTO test_uints VALUES (255, 65535, 4294967295, 18446744073709551615)")
			require.NoError(t, err)

			require.NoError(t, stmt.SetSqlQuery(ctx, "SELECT u8, u16, u32, u64 FROM test_uints"))
			r, _, err := stmt.ExecuteQuery(ctx)
			require.NoError(t, err)
			defer r.Release()

			require.True(t, r.Next())
			rec := r.RecordBatch()
			schema := rec.Schema()
			assert.Equal(t, arrow.PrimitiveTypes.Uint8.ID(), schema.Field(0).Type.ID())
			assert.Equal(t, arrow.PrimitiveTypes.Uint16.ID(), schema.Field(1).Type.ID())
			assert.Equal(t, arrow.PrimitiveTypes.Uint32.ID(), schema.Field(2).Type.ID())
			assert.Equal(t, arrow.PrimitiveTypes.Uint64.ID(), schema.Field(3).Type.ID())

			assert.Equal(t, uint8(255), rec.Column(0).(*array.Uint8).Value(0))
			assert.Equal(t, uint16(65535), rec.Column(1).(*array.Uint16).Value(0))
			assert.Equal(t, uint32(4294967295), rec.Column(2).(*array.Uint32).Value(0))
			assert.Equal(t, uint64(18446744073709551615), rec.Column(3).(*array.Uint64).Value(0))
		})

		t.Run("time_range_never_wraps", func(t *testing.T) {
			require.NoError(t, stmt.SetSqlQuery(ctx, "SELECT CAST('838:59:59' AS TIME) as t_pos, CAST('-838:59:59' AS TIME) as t_neg"))
			r, _, err := stmt.ExecuteQuery(ctx)
			require.NoError(t, err)
			defer r.Release()

			require.True(t, r.Next())
			rec := r.RecordBatch()
			assert.Equal(t, arrow.FixedWidthTypes.Time32s.ID(), rec.Schema().Field(0).Type.ID())
			col0 := rec.Column(0).(*array.Time32)
			col1 := rec.Column(1).(*array.Time32)
			assert.Equal(t, arrow.Time32(3020399), col0.Value(0))
			assert.Equal(t, arrow.Time32(-3020399), col1.Value(0))
		})

		t.Run("statement_cancel_stops_server_query", func(t *testing.T) {
			s2, err := conn.NewStatement(ctx)
			require.NoError(t, err)
			defer func() { _ = s2.Close(ctx) }()

			start := time.Now()
			require.NoError(t, s2.SetSqlQuery(ctx, "SELECT SLEEP(5)"))
			go func() {
				time.Sleep(200 * time.Millisecond)
				if c, ok := s2.(interface{ Cancel(context.Context) error }); ok {
					_ = c.Cancel(ctx)
				}
			}()

			r, _, err := s2.ExecuteQuery(ctx)
			if err == nil && r != nil {
				defer r.Release()
				require.False(t, r.Next(), "Next should return false after cancellation")
				err = r.Err()
			}
			elapsed := time.Since(start)
			require.Error(t, err, "query should be cancelled")
			assert.Contains(t, err.Error(), "interrupted")
			assert.Less(t, elapsed, 3*time.Second, "query should stop quickly when cancelled")
		})

		t.Run("cancel_idle_connection", func(t *testing.T) {
			require.NoError(t, singlestore.CancelConnection(conn, ctx))

			s, err := conn.NewStatement(ctx)
			require.NoError(t, err)
			defer func() { _ = s.Close(ctx) }()
			sCanceller, ok := s.(interface{ Cancel(context.Context) error })
			require.True(t, ok)
			require.NoError(t, sCanceller.Cancel(ctx))
		})

		t.Run("cancel_after_session_gone", func(t *testing.T) {
			conn2, err := db.Open(ctx)
			require.NoError(t, err)
			defer func() { _ = conn2.Close(ctx) }()

			s2, err := conn2.NewStatement(ctx)
			require.NoError(t, err)
			defer func() { _ = s2.Close(ctx) }()
			require.NoError(t, s2.SetSqlQuery(ctx, "SELECT CONNECTION_ID()"))
			r, _, err := s2.ExecuteQuery(ctx)
			require.NoError(t, err)
			defer r.Release()
			require.True(t, r.Next())
			rec := r.RecordBatch()
			var victimID uint64
			if col := rec.Column(0); col != nil {
				switch c := col.(type) {
				case *array.Uint64:
					victimID = c.Value(0)
				case *array.Int64:
					victimID = uint64(c.Value(0))
				case *array.Uint32:
					victimID = uint64(c.Value(0))
				case *array.Int32:
					victimID = uint64(c.Value(0))
				}
			}
			require.NotZero(t, victimID)

			rawDB, err := sql.Open("mysql", q.dsn)
			require.NoError(t, err)
			defer func() { _ = rawDB.Close() }()

			_, err = rawDB.Exec(fmt.Sprintf("KILL %d", victimID))
			require.NoError(t, err)

			require.NoError(t, singlestore.CancelConnection(conn2, ctx))

			sCanceller, ok := s2.(interface{ Cancel(context.Context) error })
			require.True(t, ok)
			require.NoError(t, sCanceller.Cancel(ctx))
		})

		t.Run("connection_registry_cleanup_on_close", func(t *testing.T) {
			initial := singlestore.ConnectionRegistryLen()
			const n = 5
			conns := make([]adbc.ConnectionWithContext, n)
			for i := 0; i < n; i++ {
				c, err := db.Open(ctx)
				require.NoError(t, err)
				conns[i] = c
			}
			assert.Equal(t, initial+n, singlestore.ConnectionRegistryLen(), "registry must contain N open connections")
			for i := 0; i < n; i++ {
				require.NoError(t, conns[i].Close(ctx))
			}
			assert.Equal(t, initial, singlestore.ConnectionRegistryLen(), "registry must return to initial size after all connections closed")
		})

		t.Run("cancel_after_close", func(t *testing.T) {
			// 1. Connection Cancel after Close = StatusInvalidState
			conn1, err := db.Open(ctx)
			require.NoError(t, err)
			s1, err := conn1.NewStatement(ctx)
			require.NoError(t, err)
			defer func() { _ = s1.Close(ctx) }()

			require.NoError(t, conn1.Close(ctx))

			err = singlestore.CancelConnection(conn1, ctx)
			require.Error(t, err)
			var adbcErr adbc.Error
			require.True(t, errors.As(err, &adbcErr), "CancelConnection after Close must return adbc.Error")
			assert.Equal(t, adbc.StatusInvalidState, adbcErr.Code)

			// 2. Statement Cancel after conn Close = StatusInvalidState and no KILL sent
			// Start a second session running a sleep query
			conn2, err := db.Open(ctx)
			require.NoError(t, err)
			defer func() { _ = conn2.Close(ctx) }()

			s2, err := conn2.NewStatement(ctx)
			require.NoError(t, err)
			defer func() { _ = s2.Close(ctx) }()
			require.NoError(t, s2.SetSqlQuery(ctx, "SELECT SLEEP(2)"))

			sleepStarted := make(chan struct{})
			sleepDone := make(chan error, 1)
			go func() {
				close(sleepStarted)
				r, _, err := s2.ExecuteQuery(ctx)
				if err == nil && r != nil {
					defer r.Release()
					for r.Next() {
					}
					err = r.Err()
				}
				sleepDone <- err
			}()

			<-sleepStarted
			time.Sleep(100 * time.Millisecond) // Ensure sleep query is actively executing on server

			// Cancel statement s1 whose connection conn1 is closed
			s1Canceller, ok := s1.(interface{ Cancel(context.Context) error })
			require.True(t, ok)
			err = s1Canceller.Cancel(ctx)
			require.Error(t, err)
			require.True(t, errors.As(err, &adbcErr), "Statement Cancel after conn Close must return adbc.Error")
			assert.Equal(t, adbc.StatusInvalidState, adbcErr.Code)

			// Wait for sleep to finish cleanly without being killed
			select {
			case sleepErr := <-sleepDone:
				assert.NoError(t, sleepErr, "second session's query must not be killed")
			case <-time.After(4 * time.Second):
				t.Fatal("timed out waiting for second session sleep query")
			}
		})
	})
}

func TestConvertToUintRangeChecked(t *testing.T) {
	// uint8 checks
	u8, err := singlestore.ConvertToUintRangeChecked[uint8](uint8(255))
	require.NoError(t, err)
	assert.Equal(t, uint8(255), u8)

	u8, err = singlestore.ConvertToUintRangeChecked[uint8]("255")
	require.NoError(t, err)
	assert.Equal(t, uint8(255), u8)

	u8, err = singlestore.ConvertToUintRangeChecked[uint8]([]byte("255"))
	require.NoError(t, err)
	assert.Equal(t, uint8(255), u8)

	_, err = singlestore.ConvertToUintRangeChecked[uint8](int64(-1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "negative")

	_, err = singlestore.ConvertToUintRangeChecked[uint8](uint64(256))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows")

	_, err = singlestore.ConvertToUintRangeChecked[uint8]("256")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overflows")

	_, err = singlestore.ConvertToUintRangeChecked[uint8](float64(10.5))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "without truncation")

	// uint16 checks
	u16, err := singlestore.ConvertToUintRangeChecked[uint16](uint16(65535))
	require.NoError(t, err)
	assert.Equal(t, uint16(65535), u16)

	_, err = singlestore.ConvertToUintRangeChecked[uint16](int32(-10))
	require.Error(t, err)

	_, err = singlestore.ConvertToUintRangeChecked[uint16](uint64(65536))
	require.Error(t, err)

	// uint32 checks
	u32, err := singlestore.ConvertToUintRangeChecked[uint32](uint32(4294967295))
	require.NoError(t, err)
	assert.Equal(t, uint32(4294967295), u32)

	_, err = singlestore.ConvertToUintRangeChecked[uint32](int64(4294967296))
	require.Error(t, err)

	// uint64 checks
	u64, err := singlestore.ConvertToUintRangeChecked[uint64](uint64(18446744073709551615))
	require.NoError(t, err)
	assert.Equal(t, uint64(18446744073709551615), u64)

	_, err = singlestore.ConvertToUintRangeChecked[uint64](int64(-1))
	require.Error(t, err)

	_, err = singlestore.ConvertToUintRangeChecked[uint64](float32(1.0))
	require.Error(t, err)
}

func TestTimeRangeParsing(t *testing.T) {
	// In-range values
	t32, err := singlestore.Time32FromString("838:59:59")
	require.NoError(t, err)
	assert.Equal(t, arrow.Time32(3020399), t32)

	t32, err = singlestore.Time32FromString("-838:59:59")
	require.NoError(t, err)
	assert.Equal(t, arrow.Time32(-3020399), t32)

	t64, err := singlestore.Time64FromString("838:00:59.000001")
	require.NoError(t, err)
	assert.Equal(t, arrow.Time64(3016859000001), t64)

	t64, err = singlestore.Time64FromString("-838:00:59.000001")
	require.NoError(t, err)
	assert.Equal(t, arrow.Time64(-3016859000001), t64)

	t64, err = singlestore.Time64FromString("838:58:59.999999")
	require.NoError(t, err)
	assert.Equal(t, arrow.Time64(3020339999999), t64)

	t64, err = singlestore.Time64FromString("-838:58:59.999999")
	require.NoError(t, err)
	assert.Equal(t, arrow.Time64(-3020339999999), t64)

	t64, err = singlestore.Time64FromString("838:59:59")
	require.NoError(t, err)
	assert.Equal(t, arrow.Time64(3020399000000), t64)

	t64, err = singlestore.Time64FromString("-838:59:59")
	require.NoError(t, err)
	assert.Equal(t, arrow.Time64(-3020399000000), t64)

	// Out-of-range values
	_, err = singlestore.Time32FromString("839:00:00")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")

	_, err = singlestore.Time32FromString("-839:00:00")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")

	_, err = singlestore.Time32FromString("12:60:00")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")

	_, err = singlestore.Time64FromString("839:00:00")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")

	_, err = singlestore.Time64FromString("838:59:59.000001")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")

	_, err = singlestore.Time64FromString("-838:59:59.000001")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")

	// Overflow protection (huge hours that wrap int64 if multiplied first)
	_, err = singlestore.Time32FromString("9223372036854775807:00:00")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")

	_, err = singlestore.Time64FromString("9223372036854775807:00:00")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")

	_, err = singlestore.Time32FromString("invalid")
	require.Error(t, err)
}
