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

package singlestore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/go-ext/variant"
	mysql "github.com/memsql/go-singlestore-driver"
)

// singlestoreTypeConverter provides SingleStore-specific type conversion enhancements
type singlestoreTypeConverter struct {
	sqlwrapper.DefaultTypeConverter
}

func normalizeDatabaseTypeName(raw string) string {
	raw = strings.ToUpper(strings.TrimSpace(raw))
	switch raw {
	case "UNSIGNED TINYINT":
		return "TINYINT UNSIGNED"
	case "UNSIGNED SMALLINT":
		return "SMALLINT UNSIGNED"
	case "UNSIGNED MEDIUMINT":
		return "MEDIUMINT UNSIGNED"
	case "UNSIGNED INT", "UNSIGNED INTEGER":
		return "INT UNSIGNED"
	case "UNSIGNED BIGINT":
		return "BIGINT UNSIGNED"
	default:
		return raw
	}
}

// ConvertRawColumnType implements TypeConverter with SingleStore-specific enhancements
func (m *singlestoreTypeConverter) ConvertRawColumnType(colType sqlwrapper.ColumnType) (arrow.DataType, bool, arrow.Metadata, error) {
	colType.DatabaseTypeName = normalizeDatabaseTypeName(colType.DatabaseTypeName)
	typeName := strings.ToUpper(colType.DatabaseTypeName)
	nullable := colType.Nullable

	switch typeName {
	case "BIT":
		// Handle BIT type as binary data
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		if colType.Length != nil {
			metadataMap["sql.length"] = fmt.Sprintf("%d", *colType.Length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.Binary, nullable, metadata, nil

	case "ENUM", "SET":
		// Handle ENUM/SET as string with special metadata
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
			"singlestore.is_enum_set":          "true",
		}

		if colType.Length != nil {
			metadataMap["sql.length"] = fmt.Sprintf("%d", *colType.Length)
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, nullable, metadata, nil

	case "TIMESTAMP", "DATETIME":
		var timestampType *arrow.TimestampType
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		if colType.Precision != nil {
			precision := *colType.Precision
			metadataMap[sqlwrapper.MetaKeyFractionalSecondsPrecision] = fmt.Sprintf("%d", precision)
			if precision > 6 {
				precision = 6
			}
			timeUnit := arrow.TimeUnit(precision / 3)
			timestampType = &arrow.TimestampType{Unit: timeUnit}
		} else {
			// No precision info available, default to microseconds (most common)
			timestampType = &arrow.TimestampType{Unit: arrow.Microsecond}
		}

		if typeName == "TIMESTAMP" {
			timestampType.TimeZone = "UTC"
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return timestampType, colType.Nullable, metadata, nil

	case "YEAR":
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.PrimitiveTypes.Int16, nullable, metadata, nil

	case "FLOAT":
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}

		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.PrimitiveTypes.Float32, nullable, metadata, nil

	case "DOUBLE":
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}
		metadata := arrow.MetadataFrom(metadataMap)

		return arrow.PrimitiveTypes.Float64, nullable, metadata, nil

	case "DECIMAL", "NUMERIC":
		if colType.Precision != nil && colType.Scale != nil {
			precision := *colType.Precision
			scale := *colType.Scale

			arrowType, err := arrow.NarrowestDecimalType(int32(precision), int32(scale))
			if err != nil {
				return nil, false, arrow.Metadata{}, fmt.Errorf("invalid decimal precision/scale (%d, %d): %w", precision, scale, err)
			}

			// Build metadata with decimal information
			metadata := arrow.MetadataFrom(map[string]string{
				sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
				sqlwrapper.MetaKeyColumnName:       colType.Name,
				sqlwrapper.MetaKeyPrecision:        fmt.Sprintf("%d", precision),
				sqlwrapper.MetaKeyScale:            fmt.Sprintf("%d", scale),
			})

			return arrowType, nullable, metadata, nil
		}

		return m.DefaultTypeConverter.ConvertRawColumnType(colType)

	case "LONGBLOB":
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}
		metadata := arrow.MetadataFrom(metadataMap)

		return arrow.BinaryTypes.LargeBinary, nullable, metadata, nil

	case "LONGTEXT":
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}
		metadata := arrow.MetadataFrom(metadataMap)

		return arrow.BinaryTypes.LargeString, nullable, metadata, nil
	case "JSON":
		metadataMap := map[string]string{
			sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
			sqlwrapper.MetaKeyColumnName:       colType.Name,
		}
		metadata := arrow.MetadataFrom(metadataMap)

		jsonType, err := extensions.NewJSONType(arrow.BinaryTypes.LargeString)
		if err != nil {
			return nil, false, arrow.Metadata{}, fmt.Errorf("error creating JSON type: %w", err)
		}

		return jsonType, nullable, metadata, nil

	default:
		// Fall back to default conversion for standard types
		return m.DefaultTypeConverter.ConvertRawColumnType(colType)
	}
}

// CreateInserter creates SingleStore-specific inserters bound to builders for enhanced performance
func (m *singlestoreTypeConverter) CreateInserter(field *arrow.Field, builder array.Builder) (sqlwrapper.Inserter, error) {
	// Check for SingleStore-specific types first
	switch field.Type.(type) {
	case *arrow.Uint8Type:
		return &singlestoreUintInserter[uint8, *array.Uint8Builder]{builder: builder.(*array.Uint8Builder)}, nil
	case *arrow.Uint16Type:
		return &singlestoreUintInserter[uint16, *array.Uint16Builder]{builder: builder.(*array.Uint16Builder)}, nil
	case *arrow.Uint32Type:
		return &singlestoreUintInserter[uint32, *array.Uint32Builder]{builder: builder.(*array.Uint32Builder)}, nil
	case *arrow.Uint64Type:
		return &singlestoreUintInserter[uint64, *array.Uint64Builder]{builder: builder.(*array.Uint64Builder)}, nil
	case *extensions.JSONType:
		return &singlestoreJSONInserter{builder: builder}, nil
	case *arrow.BinaryType:
		if dbTypeName, ok := field.Metadata.GetValue("sql.database_type_name"); ok && dbTypeName == "BIT" {
			return &singlestoreBitInserter{builder: builder.(array.BinaryLikeBuilder)}, nil
		}

		// Fall through to default for binary
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	// Time types
	case *arrow.Time32Type:
		return &singlestoreTime32Inserter{builder: builder.(*array.Time32Builder)}, nil
	case *arrow.Time64Type:
		return &singlestoreTime64Inserter{builder: builder.(*array.Time64Builder)}, nil

	default:
		// For all other types, use default inserter
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	}
}

type uintType interface {
	~uint8 | ~uint16 | ~uint32 | ~uint64
}

type uintBuilder[T uintType] interface {
	Append(T)
	AppendNull()
}

type singlestoreUintInserter[T uintType, B uintBuilder[T]] struct {
	builder B
}

func (ins *singlestoreUintInserter[T, B]) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}
	val, err := convertToUintRangeChecked[T](unwrapped)
	if err != nil {
		return err
	}
	ins.builder.Append(val)
	return nil
}

func convertToUintRangeChecked[T uintType](val any) (T, error) {
	var zero T
	var maxVal uint64
	switch any(zero).(type) {
	case uint8:
		maxVal = math.MaxUint8
	case uint16:
		maxVal = math.MaxUint16
	case uint32:
		maxVal = math.MaxUint32
	case uint64:
		maxVal = math.MaxUint64
	}

	var u uint64
	switch v := val.(type) {
	case uint64:
		u = v
	case uint:
		u = uint64(v)
	case uint32:
		u = uint64(v)
	case uint16:
		u = uint64(v)
	case uint8:
		u = uint64(v)
	case int64:
		if v < 0 {
			return zero, fmt.Errorf("cannot convert negative integer %d to unsigned type %T", v, zero)
		}
		u = uint64(v)
	case int:
		if v < 0 {
			return zero, fmt.Errorf("cannot convert negative integer %d to unsigned type %T", v, zero)
		}
		u = uint64(v)
	case int32:
		if v < 0 {
			return zero, fmt.Errorf("cannot convert negative integer %d to unsigned type %T", v, zero)
		}
		u = uint64(v)
	case int16:
		if v < 0 {
			return zero, fmt.Errorf("cannot convert negative integer %d to unsigned type %T", v, zero)
		}
		u = uint64(v)
	case int8:
		if v < 0 {
			return zero, fmt.Errorf("cannot convert negative integer %d to unsigned type %T", v, zero)
		}
		u = uint64(v)
	case []byte:
		parsed, err := strconv.ParseUint(string(v), 10, 64)
		if err != nil {
			return zero, fmt.Errorf("cannot parse %T from []byte %q: %w", zero, string(v), err)
		}
		u = parsed
	case string:
		parsed, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return zero, fmt.Errorf("cannot parse %T from string %q: %w", zero, v, err)
		}
		u = parsed
	case float64, float32:
		return zero, fmt.Errorf("cannot convert float %v to integer type %T without truncation", val, zero)
	default:
		return zero, fmt.Errorf("cannot convert %T to %T", val, zero)
	}

	if u > maxVal {
		return zero, fmt.Errorf("value %d overflows %T (maximum %d)", u, zero, maxVal)
	}
	return T(u), nil
}

// SingleStore-specific inserters
type singlestoreTime64Inserter struct {
	builder *array.Time64Builder
}

func (ins *singlestoreTime64Inserter) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	timeType := ins.builder.Type().(*arrow.Time64Type)
	if timeType.Unit != arrow.Microsecond {
		return fmt.Errorf("unsupported Time64 unit: %v", timeType.Unit)
	}

	val, err := convertToTime64(unwrapped)
	if err != nil {
		return err
	}
	ins.builder.Append(val)
	return nil
}

// convertToTime64 converts a SQL value to arrow.Time64 type
func convertToTime64(val any) (arrow.Time64, error) {
	switch v := val.(type) {
	case time.Time:
		return arrow.Time64(int64(v.Hour())*3600000000 + int64(v.Minute()*60000000) + int64(v.Second()*1000000) + int64(v.Nanosecond())/1000), nil
	case []byte:
		return time64FromString(string(v))
	case string:
		return time64FromString(v)
	default:
		return 0, fmt.Errorf("cannot convert %T to Time64, expected time.Time", val)
	}
}

func time64FromString(timeStr string) (arrow.Time64, error) {
	isNegative := false

	// Handle the negative sign
	if strings.HasPrefix(timeStr, "-") {
		isNegative = true
		timeStr = strings.TrimPrefix(timeStr, "-")
	}

	// Split into HH, MM, and SS.FFFFFF
	parts := strings.Split(timeStr, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid time format: expected HH:MM:SS[.frac], got %s", timeStr)
	}

	hours, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing hours: %w", err)
	}

	minutes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing minutes: %w", err)
	}

	// Separate seconds from fractional microseconds
	secParts := strings.Split(parts[2], ".")
	seconds, err := strconv.ParseInt(secParts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing seconds: %w", err)
	}

	var microseconds int64 = 0

	// Handle the fractional part if it exists
	if len(secParts) > 1 {
		fractionStr := secParts[1]

		// SingleStore supports up to 6 digits (microseconds).
		// We must pad the string with zeros on the right so ".7" becomes "700000".
		if len(fractionStr) > 6 {
			fractionStr = fractionStr[:6] // Truncate to 6 digits if it's too long
		} else if len(fractionStr) < 6 {
			fractionStr = fractionStr + strings.Repeat("0", 6-len(fractionStr))
		}

		microseconds, err = strconv.ParseInt(fractionStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("error parsing microseconds: %w", err)
		}
	}

	if hours < 0 || hours > 838 || minutes < 0 || seconds < 0 || microseconds < 0 ||
		minutes >= 60 || seconds >= 60 {
		return 0, fmt.Errorf("time value out of range [-838:59:59, 838:59:59]: %s", timeStr)
	}

	// Calculate total microseconds
	totalMicroseconds := (hours * 3600000000) +
		(minutes * 60000000) +
		(seconds * 1000000) +
		microseconds

	const maxTime64Microseconds = int64(838*3600+59*60+59) * 1000000 // 3020399000000
	if totalMicroseconds > maxTime64Microseconds {
		return 0, fmt.Errorf("time value out of range [-838:59:59, 838:59:59]: %s", timeStr)
	}

	// Reapply negative sign
	if isNegative {
		totalMicroseconds = -totalMicroseconds
	}

	return arrow.Time64(totalMicroseconds), nil
}

type singlestoreTime32Inserter struct {
	builder *array.Time32Builder
}

func (ins *singlestoreTime32Inserter) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	timeType := ins.builder.Type().(*arrow.Time32Type)
	if timeType.Unit != arrow.Second {
		return fmt.Errorf("unsupported Time32 unit: %v", timeType.Unit)
	}

	val, err := convertToTime32(unwrapped)
	if err != nil {
		return err
	}
	ins.builder.Append(val)
	return nil
}

func unwrap(val any) (any, error) {
	if v, ok := val.(driver.Valuer); ok {
		return v.Value()
	}
	return val, nil
}

// convertToTime32 converts a SQL value to arrow.Time32 type
func convertToTime32(val any) (arrow.Time32, error) {
	switch v := val.(type) {
	case time.Time:
		return arrow.Time32(v.Hour()*3600 + v.Minute()*60 + v.Second()), nil
	case []byte:
		return time32FromString(string(v))
	case string:
		return time32FromString(v)
	default:
		return 0, fmt.Errorf("cannot convert %T to Time32, expected time.Time", val)
	}
}

func time32FromString(timeStr string) (arrow.Time32, error) {
	isNegative := false

	// Check for and remove the negative sign
	if strings.HasPrefix(timeStr, "-") {
		isNegative = true
		timeStr = strings.TrimPrefix(timeStr, "-")
	}

	// Split the string into Hours, Minutes, and Seconds
	parts := strings.Split(timeStr, ":")
	if len(parts) != 3 {
		return 0, fmt.Errorf("invalid time format: expected HH:MM:SS, got %s", timeStr)
	}

	// Parse each part into an int64
	hours, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing hours: %w", err)
	}

	minutes, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing minutes: %w", err)
	}

	seconds, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing seconds: %w", err)
	}

	if hours < 0 || hours > 838 || minutes < 0 || seconds < 0 ||
		minutes >= 60 || seconds >= 60 {
		return 0, fmt.Errorf("time value out of range [-838:59:59, 838:59:59]: %s", timeStr)
	}

	// Calculate total seconds (Hours * 3600 + Minutes * 60 + Seconds)
	totalSeconds := (hours * 3600) + (minutes * 60) + seconds
	const maxTime32Seconds = int64(838*3600 + 59*60 + 59) // 3020399
	if totalSeconds > maxTime32Seconds {
		return 0, fmt.Errorf("time value out of range [-838:59:59, 838:59:59]: %s", timeStr)
	}

	// Reapply the negative sign if the original string was negative
	if isNegative {
		totalSeconds = -totalSeconds
	}

	return arrow.Time32(totalSeconds), nil
}

type singlestoreJSONInserter struct {
	builder array.Builder
}

func (ins *singlestoreJSONInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := sqlValue.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte for singlestore json inserter, got %T", sqlValue)
	}

	// For extension types, we need to use AppendValueFromString
	// since the ExtensionBuilder doesn't implement StringLikeBuilder.Append
	return ins.builder.AppendValueFromString(string(t))
}

type singlestoreBitInserter struct {
	builder array.BinaryLikeBuilder
}

func (ins *singlestoreBitInserter) AppendValue(sqlValue any) error {
	if sqlValue == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := sqlValue.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte for singlestore bit inserter, got %T", sqlValue)
	}

	ins.builder.Append(t)
	return nil
}

// ConvertArrowToGo implements SingleStore-specific Arrow value to Go value conversion
func (m *singlestoreTypeConverter) ConvertArrowToGo(arrowArray arrow.Array, index int, field *arrow.Field) (any, error) {
	if arrowArray.DataType().ID() == arrow.NULL || arrowArray.IsNull(index) {
		return nil, nil
	}

	// Handle SingleStore-specific Arrow to Go conversions
	switch a := arrowArray.(type) {
	case *extensions.JSONArray:
		// Handle JSON extension type arrays
		jsonStr := a.ValueStr(index)
		v := variant.New(jsonStr)
		return v, nil

	case *array.Time32:
		value := int64(a.Value(index))
		unit := a.DataType().(*arrow.Time32Type).Unit
		return formatSingleStoreTime(value, unit)

	case *array.Time64:
		value := int64(a.Value(index))
		unit := a.DataType().(*arrow.Time64Type).Unit
		return formatSingleStoreTime(value, unit)

	case *array.Timestamp:
		timestampType := a.DataType().(*arrow.TimestampType)
		rawValue := a.Value(index)
		t := rawValue.ToTime(timestampType.Unit)

		// For nanosecond precision, truncate to microseconds
		if timestampType.Unit == arrow.Nanosecond {
			microseconds := t.UnixMicro()
			converted := time.UnixMicro(microseconds).UTC()
			return converted, nil
		}

		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)

	default:
		// For all other types, use default conversion
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)
	}
}

func ConvertArrowToCSV(typ arrow.DataType, col arrow.Array) (result []string, handled bool) {
	var err error
	switch typ.ID() {
	case arrow.TIMESTAMP:
		result = make([]string, col.Len())
		arr := col.(*array.Timestamp)
		t := typ.(*arrow.TimestampType)

		for i := 0; i < arr.Len(); i++ {
			if !arr.IsValid(i) {
				result[i] = "\\N"
				continue
			}

			result[i] = arr.Value(i).ToTime(t.Unit).Format("2006-01-02 15:04:05.999999")
		}
		return result, true

	case arrow.STRING:
		result = make([]string, col.Len())
		arr := col.(*array.String)
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsValid(i) {
				result[i] = "\\N"
				continue
			}

			result[i] = strings.ReplaceAll(arr.Value(i), `\`, `\\`)
		}
		return result, true

	case arrow.LARGE_STRING:
		result = make([]string, col.Len())
		arr := col.(*array.LargeString)
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsValid(i) {
				result[i] = "\\N"
				continue
			}

			result[i] = strings.ReplaceAll(arr.Value(i), `\`, `\\`)
		}
		return result, true

	case arrow.STRING_VIEW:
		result = make([]string, col.Len())
		arr := col.(*array.StringView)
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsValid(i) {
				result[i] = "\\N"
				continue
			}

			result[i] = strings.ReplaceAll(arr.Value(i), `\`, `\\`)
		}
		return result, true

	case arrow.BINARY_VIEW:
		// Convert BINARY_VIEW values to base64.
		// BINARY, FIXED_SIZE_BINARY, and LARGE_BINARY are intentionally excluded,
		// since the default converter already serializes them as base64.
		result = make([]string, col.Len())
		arr := col.(*array.BinaryView)
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsValid(i) {
				result[i] = "\\N"
				continue
			}

			result[i] = base64.StdEncoding.EncodeToString(arr.Value(i))
		}
		return result, true

	case arrow.TIME32:
		result = make([]string, col.Len())
		arr := col.(*array.Time32)
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsValid(i) {
				result[i] = "\\N"
				continue
			}

			value := int64(arr.Value(i))
			unit := arr.DataType().(*arrow.Time32Type).Unit
			result[i], err = formatSingleStoreTime(value, unit)
			if err != nil {
				return nil, false
			}
		}
		return result, true

	case arrow.TIME64:
		result = make([]string, col.Len())
		arr := col.(*array.Time64)
		for i := 0; i < arr.Len(); i++ {
			if !arr.IsValid(i) {
				result[i] = "\\N"
				continue
			}

			value := int64(arr.Value(i))
			unit := arr.DataType().(*arrow.Time64Type).Unit
			result[i], err = formatSingleStoreTime(value, unit)
			if err != nil {
				return nil, false
			}
		}
		return result, true

	default:
		return nil, false
	}
}

func formatSingleStoreTime(t int64, unit arrow.TimeUnit) (string, error) {
	switch unit {
	case arrow.Second:
		return formatSingleStoreTimeInMicros(t * 1000000), nil
	case arrow.Millisecond:
		return formatSingleStoreTimeInMicros(t * 1000), nil
	case arrow.Microsecond:
		return formatSingleStoreTimeInMicros(t), nil
	case arrow.Nanosecond:
		return formatSingleStoreTimeInMicros(t / 1000), nil
	default:
		return "", fmt.Errorf("unsupported Time unit: %v", unit)
	}
}

// formatSingleStoreTimeInMicros takes a time value in microseconds and returns its string representation.
func formatSingleStoreTimeInMicros(t int64) string {
	sign := ""
	if t < 0 {
		sign = "-"
		t = -t
	}

	micros := t % 1000000
	t = t / 1000000
	secs := t % 60
	t = t / 60
	mins := t % 60
	t = t / 60
	hours := t

	return fmt.Sprintf("%s%d:%02d:%02d.%06d", sign, hours, mins, secs, micros)
}

// singlestoreConnectionImpl extends sqlwrapper connection with DbObjectsEnumerator
type singlestoreConnectionImpl struct {
	*sqlwrapper.ConnectionImplBase // Embed sqlwrapper connection for all standard functionality

	version      string
	connID       atomic.Uint64
	aggregatorID atomic.Int64
}

func (c *singlestoreConnectionImpl) Cancel(ctx context.Context) error {
	connID := c.connID.Load()
	if c.Db == nil || connID == 0 {
		return c.ErrorHelper.InvalidState("cannot cancel: connection is closed or connection ID is not available")
	}
	// Note: sqlwrapper.ExecuteQuery uses context.Background() so that context cancellation
	// does not close the pinned *sql.Conn. We therefore issue KILL QUERY <conn_id> <node_id>
	// directly over a separate pool connection (matching go-singlestore-driver's killQuery).
	var killQuery string
	if aggID := c.aggregatorID.Load(); aggID > 0 {
		killQuery = fmt.Sprintf("KILL QUERY %d %d", connID, aggID)
	} else {
		killQuery = fmt.Sprintf("KILL QUERY %d", connID)
	}
	_, err := c.Db.ExecContext(ctx, killQuery)
	if err != nil {
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) && myErr.Number == 1094 {
			return nil
		}
		return c.ErrorHelper.WrapIO(err, "failed to kill query on connection %d", connID)
	}
	return nil
}

func (c *singlestoreConnectionImpl) NewStatement(ctx context.Context) (adbc.StatementWithContext, error) {
	stmt, err := c.ConnectionImplBase.NewStatement(ctx)
	if err != nil {
		return nil, err
	}
	return &singlestoreStatement{
		StatementWithContext: stmt,
		conn:                 c,
	}, nil
}

type singlestoreStatement struct {
	adbc.StatementWithContext
	conn *singlestoreConnectionImpl
}

func (s *singlestoreStatement) Cancel(ctx context.Context) error {
	return s.conn.Cancel(ctx)
}

func (s *singlestoreStatement) GetOption(ctx context.Context, key string) (string, error) {
	if opts, ok := s.StatementWithContext.(adbc.GetSetOptionsWithContext); ok {
		return opts.GetOption(ctx, key)
	}
	return "", adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *singlestoreStatement) GetOptionBytes(ctx context.Context, key string) ([]byte, error) {
	if opts, ok := s.StatementWithContext.(adbc.GetSetOptionsWithContext); ok {
		return opts.GetOptionBytes(ctx, key)
	}
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *singlestoreStatement) GetOptionDouble(ctx context.Context, key string) (float64, error) {
	if opts, ok := s.StatementWithContext.(adbc.GetSetOptionsWithContext); ok {
		return opts.GetOptionDouble(ctx, key)
	}
	return 0, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *singlestoreStatement) GetOptionInt(ctx context.Context, key string) (int64, error) {
	if opts, ok := s.StatementWithContext.(adbc.GetSetOptionsWithContext); ok {
		return opts.GetOptionInt(ctx, key)
	}
	return 0, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *singlestoreStatement) SetOptionBytes(ctx context.Context, key string, val []byte) error {
	if opts, ok := s.StatementWithContext.(adbc.GetSetOptionsWithContext); ok {
		return opts.SetOptionBytes(ctx, key, val)
	}
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *singlestoreStatement) SetOptionDouble(ctx context.Context, key string, val float64) error {
	if opts, ok := s.StatementWithContext.(adbc.GetSetOptionsWithContext); ok {
		return opts.SetOptionDouble(ctx, key, val)
	}
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *singlestoreStatement) SetOptionInt(ctx context.Context, key string, val int64) error {
	if opts, ok := s.StatementWithContext.(adbc.GetSetOptionsWithContext); ok {
		return opts.SetOptionInt(ctx, key, val)
	}
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *singlestoreStatement) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	if es, ok := s.StatementWithContext.(adbc.StatementExecuteSchemaWithContext); ok {
		return es.ExecuteSchema(ctx)
	}
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

// implements BulkIngester interface
var _ sqlwrapper.BulkIngester = (*singlestoreConnectionImpl)(nil)

// implements DbObjectsEnumerator interface
var _ driverbase.DbObjectsEnumerator = (*singlestoreConnectionImpl)(nil)

// implements CurrentNameSpacer interface
var _ driverbase.CurrentNamespacer = (*singlestoreConnectionImpl)(nil)

// implements TableTypeLister interface
var _ driverbase.TableTypeLister = (*singlestoreConnectionImpl)(nil)

var (
	connectionsMu sync.RWMutex
	connections   = make(map[*driverbase.ConnectionImplBase]*singlestoreConnectionImpl)
)

func registerConnection(base *driverbase.ConnectionImplBase, impl *singlestoreConnectionImpl) {
	connectionsMu.Lock()
	defer connectionsMu.Unlock()
	connections[base] = impl
}

func unregisterConnection(base *driverbase.ConnectionImplBase) {
	connectionsMu.Lock()
	defer connectionsMu.Unlock()
	delete(connections, base)
}

func (c *singlestoreConnectionImpl) Close(ctx context.Context) error {
	defer unregisterConnection(c.Base())
	c.connID.Swap(0)
	c.aggregatorID.Store(0)
	return c.ConnectionImplBase.Close(ctx)
}

func GetConnectionImpl(cnxn any) *singlestoreConnectionImpl {
	if impl, ok := cnxn.(*singlestoreConnectionImpl); ok {
		return impl
	}
	if ci, ok := cnxn.(driverbase.ConnectionImpl); ok {
		connectionsMu.RLock()
		defer connectionsMu.RUnlock()
		return connections[ci.Base()]
	}
	return nil
}

func CancelConnection(cnxn any, ctx context.Context) error {
	if impl := GetConnectionImpl(cnxn); impl != nil {
		return impl.Cancel(ctx)
	}
	if c, ok := cnxn.(interface{ Cancel(context.Context) error }); ok {
		return c.Cancel(ctx)
	}
	return adbc.Error{
		Code: adbc.StatusInvalidState,
		Msg:  "cannot cancel: connection is closed or not found",
	}
}

func CancelStatement(stmt any, ctx context.Context) error {
	if c, ok := stmt.(interface{ Cancel(context.Context) error }); ok {
		return c.Cancel(ctx)
	}
	return adbc.Error{
		Code: adbc.StatusInvalidState,
		Msg:  "cannot cancel: statement is closed or not found",
	}
}

// singlestoreConnectionFactory creates SingleStore connections
type singlestoreConnectionFactory struct{}

// CreateConnection implements sqlwrapper.ConnectionFactory
func (f *singlestoreConnectionFactory) CreateConnection(
	ctx context.Context,
	conn *sqlwrapper.ConnectionImplBase,
) (sqlwrapper.ConnectionImpl, error) {
	var connID uint64
	var aggregatorID sql.NullInt64
	if err := conn.Conn.QueryRowContext(ctx, "SELECT CONNECTION_ID() :> BIGINT, AGGREGATOR_ID() :> BIGINT").Scan(&connID, &aggregatorID); err != nil {
		return nil, conn.ErrorHelper.WrapIO(err, "failed to get connection and aggregator IDs")
	}

	impl := &singlestoreConnectionImpl{
		ConnectionImplBase: conn,
	}
	impl.connID.Store(connID)
	if aggregatorID.Valid {
		impl.aggregatorID.Store(aggregatorID.Int64)
	}
	registerConnection(conn.Base(), impl)
	return impl, nil
}

// NewDriver constructs the ADBC Driver for "singlestore".
func NewDriver(alloc memory.Allocator) driverbase.DriverWithContext {
	vendorName := "SingleStore"
	typeConverter := &singlestoreTypeConverter{
		DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{VendorName: vendorName},
	}

	driver := sqlwrapper.NewDriver(alloc, "mysql", vendorName, NewSingleStoreDBFactory(), typeConverter).
		WithConnectionFactory(&singlestoreConnectionFactory{}).
		WithErrorInspector(SingleStoreErrorInspector{})
	driver.DriverInfo.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:      "ADBC Driver for SingleStore",
		adbc.InfoVendorSql:       true,
		adbc.InfoVendorSubstrait: false,
	})

	return driver
}
