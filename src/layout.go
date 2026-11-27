package postgres

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"

	"github.com/tycheg/reasonr-engine/port"
)

// Storage (RFC §31.2): a table per entity or projection type, a row per
// entity or projection row — the state in state jsonb (canonical JSON),
// the ciphertexts of personal values in sealed, their subjects in subjects
// (a GIN index: forget finds rows by them) and columns only for index keys.
// The column types keep the model's order: strings — collate "C"
// (bytewise), timestamps — numeric nanoseconds, enumerations — the value
// number.

var colTypes = [...]string{
	port.ColInt: "bigint", port.ColFloat: "double precision", port.ColNumeric: "numeric", port.ColNanos: "numeric",
	port.ColText: `text collate "C"`, port.ColBytes: "bytea", port.ColBool: "boolean", port.ColUUID: "uuid", port.ColOpaque: "bytea",
	port.ColBlind: "bytea",
}

// infoTypes is the column type in information_schema by kind.
var infoTypes = [...]string{
	port.ColInt: "bigint", port.ColFloat: "double precision", port.ColNumeric: "numeric", port.ColNanos: "numeric",
	port.ColText: "text", port.ColBytes: "bytea", port.ColBool: "boolean", port.ColUUID: "uuid", port.ColOpaque: "bytea",
	port.ColBlind: "bytea",
}

// tableName: e_ for entities and actors, p_ for projections; the type name
// in snake_case.
func tableName(t *port.Table) string {
	if t.Projection {
		return "p_" + snake(t.Type)
	}
	return "e_" + snake(t.Type)
}

func snake(s string) string {
	// A full name (Ordering.Order): parts joined with "__" — ordering__order.
	if strings.Contains(s, ".") {
		parts := strings.Split(s, ".")
		for i, p := range parts {
			parts[i] = snake(p)
		}
		return strings.Join(parts, "__")
	}
	var b strings.Builder
	rs := []rune(s)
	for i, r := range rs {
		if unicode.IsUpper(r) && i > 0 && (unicode.IsLower(rs[i-1]) || i+1 < len(rs) && unicode.IsLower(rs[i+1])) {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

// subjectsIndex is the name of the GIN index on the subjects column.
func subjectsIndex(t *port.Table) string { return limitName("g_" + tableName(t) + "__subjects") }

// indexName is the index name in the database: u_ for unique ones (error
// 23505 is matched with the model index by it), i_ for the others.
func indexName(t *port.Table, ix *port.Index) string {
	prefix := "i_"
	if ix.Unique {
		prefix = "u_"
	}
	return limitName(prefix + tableName(t) + "__" + ix.Name)
}

// limitName shortens a name longer than PostgreSQL's 63 bytes with a hash.
func limitName(n string) string {
	if len(n) > 63 {
		sum := sha256.Sum256([]byte(n))
		n = n[:50] + "_" + hex.EncodeToString(sum[:6])
	}
	return n
}

// subjectTexts gives the values of the subjects column: "Type:id".
func subjectTexts(refs []port.SubjectRef) []string {
	if refs == nil {
		return nil
	}
	out := make([]string, len(refs))
	for i, r := range refs {
		out[i] = r.Type + ":" + r.ID
	}
	return out
}

// subjectRefs reads the subjects column.
func subjectRefs(texts []string) []port.SubjectRef {
	if texts == nil {
		return nil
	}
	out := make([]port.SubjectRef, len(texts))
	for i, s := range texts {
		t, id, _ := strings.Cut(s, ":")
		out[i] = port.SubjectRef{Type: t, ID: id}
	}
	return out
}
