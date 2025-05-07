package genql

import (
	"bytes"
	"fmt"

	"maps"

	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

type (
	Locator struct {
		Map  *Map
		Rows []int
	}
	Partition map[string]Locator
)

func StraightJoin(query *Query, left, right []any, expr sqlparser.Expr) ([]any, error) {
	kl, kr := Key(expr)
	all := make([]any, 0, len(left)+len(right))
	all = append(all, left...)
	all = append(all, right...)
	leftPartition, err := Partitionize(all, kl)
	if err != nil {
		return nil, err
	}
	rightPartition, err := Partitionize(all, kr)
	if err != nil {
		return nil, err
	}

	out := make([]any, 0)
	for _, l := range leftPartition {
		for _, r := range rightPartition {
			current := make(Map)
			maps.Copy(current, *l.Map)
			maps.Copy(current, *r.Map)
			rs, err := Expr(query, current, expr, nil)
			if err != nil {
				return nil, err
			}
			rsValue, ok := rs.(bool)
			if !ok {
				return nil, INVALID_TYPE.Extend(fmt.Sprintf("failed to build `JOIN` expression, expected boolean but found %T", left))
			}
			if rsValue {
				for _, li := range l.Rows {
					for _, ri := range r.Rows {
						current := make(Map)
						maps.Copy(current, all[li].(Map))
						maps.Copy(current, all[ri].(Map))
						out = append(out, current)
					}
				}
			}
		}
	}

	return out, nil
}

func Partitionize(rows []any, keys []string) (Partition, error) {
	partition := make(Partition)

	segments := make([][]string, len(keys))

	for i := 0; i < len(keys); i++ {
		segments[i] = SplitKey(keys[i])
	}

	var buffer bytes.Buffer

LOOP:
	for i, r := range rows {
		mapper := make(Map)
		buffer.Reset()
		for i, segment := range segments {
			v, err := ExtractKeys(r.(Map), segment...)
			if err != nil {
				if err.Error() == "key not found" {
					continue LOOP
				}
				return nil, err
			}
			ref := mapper
			for i := 0; i < len(segment)-1; i++ {
				k := segment[i]
				v, ok := ref[k]
				if !ok {
					ref[k] = make(Map)
					v = ref[k]
				}
				ref = v.(Map)
			}
			ref[segment[len(segment)-1]] = v
			buffer.WriteString(fmt.Sprintf(`"%s":"%v",`, keys[i], v))
		}
		key := buffer.String()
		v, ok := partition[key]
		if !ok {
			locator := new(Locator)
			locator.Map = &mapper
			locator.Rows = make([]int, 0)
			partition[key] = *locator
			v = partition[key]
		}
		v.Rows = append(partition[key].Rows, i)
		partition[key] = v
	}

	return partition, nil
}

func ExtractKeys(row Map, segments ...string) (any, error) {
	v, ok := row[segments[0]]
	if !ok {
		return nil, fmt.Errorf("key not found")
	}
	if v, ok := v.(Map); ok {
		return ExtractKeys(v, segments[1:]...)
	}
	if len(segments) > 1 {
		return nil, fmt.Errorf("cannot propegate further")
	}
	return v, nil
}

func SplitKey(key string) []string {
	var buffer bytes.Buffer
	out := make([]string, 0)

	jump := false

	for _, r := range key {
		if r == '\\' {
			buffer.WriteRune(r)
			jump = true
			continue
		}
		if r != '.' || jump {
			jump = false
			buffer.WriteRune(r)
			continue
		}
		out = append(out, buffer.String())
		buffer.Reset()
	}

	if buffer.Len() != 0 {
		out = append(out, buffer.String())
		buffer.Reset()
	}

	return out
}

func Key(expr sqlparser.Expr) ([]string, []string) {
	left := make([]string, 0)
	right := make([]string, 0)
	switch expr := expr.(type) {
	case *sqlparser.AndExpr:
		{
			ll, lr := Key(expr.Left)
			rl, rr := Key(expr.Right)

			left = append(left, ll...)
			left = append(left, lr...)
			right = append(right, rl...)
			right = append(right, rr...)
		}
	case *sqlparser.OrExpr:
		{
			ll, lr := Key(expr.Left)
			rl, rr := Key(expr.Right)

			left = append(left, ll...)
			left = append(left, lr...)
			right = append(right, rl...)
			right = append(right, rr...)
		}
	case *sqlparser.ComparisonExpr:
		{
			ll, lr := Key(expr.Left)
			rl, rr := Key(expr.Right)

			left = append(left, ll...)
			left = append(left, lr...)
			right = append(right, rl...)
			right = append(right, rr...)
		}
	case *sqlparser.BetweenExpr:
		{
			ll, lr := Key(expr.Left)

			left = append(left, ll...)
			left = append(left, lr...)
		}
	case *sqlparser.BinaryExpr:
		{
			ll, lr := Key(expr.Left)
			rl, rr := Key(expr.Right)

			left = append(left, ll...)
			left = append(left, lr...)
			right = append(right, rl...)
			right = append(right, rr...)
		}
	case *sqlparser.NullVal:
		{
			return nil, nil
		}

	case *sqlparser.ColName:
		{
			qualifier, name, err := BuildColumnName(expr)
			if err != nil {
				return nil, nil
			}
			columnName := name
			if len(qualifier) > 0 {
				columnName = fmt.Sprintf("%s.%s", qualifier, name)
			}
			return []string{columnName}, []string{}
		}
	}
	return left, right
}
