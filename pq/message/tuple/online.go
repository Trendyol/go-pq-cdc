package tuple

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"

	"github.com/jackc/pgx/v5/pgtype"
)

// NewOnlineTypeMap keeps JSON array element codecs lossless too; it never
// mutates pgtype's shared defaults or the legacy tuple decoder.
func NewOnlineTypeMap() *pgtype.Map {
	m := pgtype.NewMap()
	unmarshal := func(data []byte, v any) error {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		return decoder.Decode(v)
	}
	jt := &pgtype.Type{Name: "json", OID: pgtype.JSONOID, Codec: &pgtype.JSONCodec{Marshal: json.Marshal, Unmarshal: unmarshal}}
	jbt := &pgtype.Type{Name: "jsonb", OID: pgtype.JSONBOID, Codec: &pgtype.JSONBCodec{Marshal: json.Marshal, Unmarshal: unmarshal}}
	m.RegisterType(jt)
	m.RegisterType(jbt)
	m.RegisterType(&pgtype.Type{Name: "_json", OID: pgtype.JSONArrayOID, Codec: &pgtype.ArrayCodec{ElementType: jt}})
	m.RegisterType(&pgtype.Type{Name: "_jsonb", OID: pgtype.JSONBArrayOID, Codec: &pgtype.ArrayCodec{ElementType: jbt}})
	return m
}

// OnlineBusinessOID is the qualified POC value profile, not all PostgreSQL
// types. UUID is deliberately absent: it is only an internal control codec.
func OnlineBusinessOID(oid uint32) bool {
	switch oid {
	case pgtype.BoolOID, pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID, pgtype.TextOID, pgtype.VarcharOID, pgtype.BPCharOID, pgtype.Float4OID, pgtype.Float8OID, pgtype.JSONOID, pgtype.JSONBOID:
		return true
	}
	return onlineArrayOID(oid)
}
func onlineArrayOID(oid uint32) bool {
	switch oid {
	case pgtype.BoolArrayOID, pgtype.Int2ArrayOID, pgtype.Int4ArrayOID, pgtype.Int8ArrayOID, pgtype.TextArrayOID, pgtype.VarcharArrayOID, pgtype.BPCharArrayOID, pgtype.Float4ArrayOID, pgtype.Float8ArrayOID, pgtype.JSONArrayOID, pgtype.JSONBArrayOID:
		return true
	}
	return false
}
func onlineWireOID(oid uint32) bool { return OnlineBusinessOID(oid) || oid == pgtype.UUIDOID }

// DecodeOnlineText is shared by online READ/CDC only. JSON numbers remain exact;
// PostgreSQL non-finite floats use its JSON strings Infinity/-Infinity/NaN.
// Legacy global codecs are unchanged. Unqualified OIDs/shapes fail closed.
func DecodeOnlineText(types *pgtype.Map, oid uint32, raw []byte) (any, error) {
	if !onlineWireOID(oid) {
		return nil, fmt.Errorf("online unqualified column OID %d; use the documented POC type profile", oid)
	}
	if raw == nil {
		return nil, nil
	}
	if oid == pgtype.JSONOID || oid == pgtype.JSONBOID {
		var value any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return nil, fmt.Errorf("online JSON trailing input")
		}
		return value, nil
	}
	typ, ok := types.TypeForOID(oid)
	if !ok {
		return nil, fmt.Errorf("online missing codec for qualified OID %d", oid)
	}
	if onlineArrayOID(oid) {
		var array pgtype.Array[any]
		if err := types.Scan(oid, pgtype.TextFormatCode, raw, &array); err != nil {
			return nil, err
		}
		if len(array.Dims) > 1 || len(array.Dims) == 1 && array.Dims[0].LowerBound != 1 {
			return nil, fmt.Errorf("online arrays require one dimension and lower bound 1")
		}
		if len(array.Elements) == 0 {
			return []any{}, nil
		}
		return canonicalOnlineValue(array.Elements), nil
	}
	value, err := typ.Codec.DecodeValue(types, oid, pgtype.TextFormatCode, raw)
	if err != nil {
		return nil, err
	}
	return canonicalOnlineValue(value), nil
}
func canonicalOnlineValue(value any) any {
	switch v := value.(type) {
	case float32:
		if math.IsNaN(float64(v)) {
			return "NaN"
		}
		if math.IsInf(float64(v), 1) {
			return "Infinity"
		}
		if math.IsInf(float64(v), -1) {
			return "-Infinity"
		}
	case float64:
		if math.IsNaN(v) {
			return "NaN"
		}
		if math.IsInf(v, 1) {
			return "Infinity"
		}
		if math.IsInf(v, -1) {
			return "-Infinity"
		}
	case []any:
		for i, element := range v {
			v[i] = canonicalOnlineValue(element)
		}
	}
	return value
}
func DecodeOnlineData(types *pgtype.Map, data *Data, columns []RelationColumn) (map[string]any, error) {
	if data == nil || len(data.Columns) != len(columns) {
		return nil, fmt.Errorf("online incomplete tuple")
	}
	image := make(map[string]any, len(columns))
	for i, column := range data.Columns {
		if !onlineWireOID(columns[i].DataType) {
			return nil, fmt.Errorf("online unqualified column OID %d", columns[i].DataType)
		}
		switch column.DataType {
		case DataTypeNull:
			image[columns[i].Name] = nil
		case DataTypeText:
			value, err := DecodeOnlineText(types, columns[i].DataType, column.Data)
			if err != nil {
				return nil, err
			}
			image[columns[i].Name] = value
		default:
			return nil, fmt.Errorf("online unresolved TOAST/binary image")
		}
	}
	return image, nil
}
