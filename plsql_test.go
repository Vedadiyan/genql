// Copyright 2023 Pouya Vedadiyan
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package genql

import (
	"reflect"
	"testing"

	"github.com/vedadiyan/sqlparser/pkg/sqlparser"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name      string
		data      Map
		query     string
		options   []QueryOption
		wantErr   bool
		checkFunc func(*Query) bool
	}{
		{
			name: "Basic Query Creation",
			data: Map{
				"test": []Map{
					{"id": 1, "name": "test1"},
					{"id": 2, "name": "test2"},
				},
			},
			query:   "SELECT * FROM test",
			options: nil,
			wantErr: false,
			checkFunc: func(q *Query) bool {
				data := q.data["test"]
				if data == nil {
					return false
				}
				testData, ok := data.([]Map)
				return q != nil && ok && len(testData) == 2 && testData[0]["id"] == 1
			},
		},
		{
			name: "With Wrapped Option",
			data: Map{
				"test": []Map{
					{"id": 1, "name": "test1"},
					{"id": 2, "name": "test2"},
				},
			},
			query:   "SELECT * FROM test",
			options: []QueryOption{Wrapped()},
			wantErr: false,
			checkFunc: func(q *Query) bool {
				root, ok := q.data["root"].(Map)
				if !ok {
					return false
				}
				testData, ok := root["test"].([]Map)
				return q != nil && q.options.wrapped && ok && len(testData) == 2 && testData[0]["id"] == 1
			},
		},
		{
			name: "With Postgres Dialect",
			data: Map{
				"test": []Map{
					{"id": 1, "name": "test1"},
					{"id": 2, "name": "test2"},
				},
			},
			query:   `SELECT * FROM "test"`,
			options: []QueryOption{PostgresEscapingDialect()},
			wantErr: false,
			checkFunc: func(q *Query) bool {
				data := q.data["test"]
				if data == nil {
					return false
				}
				testData, ok := data.([]Map)
				return q != nil && q.options.postgresEscapingDialect && ok && len(testData) == 2
			},
		},
		{
			name: "Invalid SQL Query",
			data: Map{
				"test": []Map{
					{"id": 1, "name": "test1"},
				},
			},
			query:     "INVALID SQL",
			options:   nil,
			wantErr:   true,
			checkFunc: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := New(tt.data, tt.query, tt.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !tt.checkFunc(query) {
				t.Errorf("New() failed validation check")
			}
		})
	}
}
func TestQueryOptions(t *testing.T) {
	tests := []struct {
		name  string
		opt   QueryOption
		check func(*Query) bool
	}{
		{
			name: "Wrapped Option",
			opt:  Wrapped(),
			check: func(q *Query) bool {
				return q.options.wrapped
			},
		},
		{
			name: "Postgres Dialect Option",
			opt:  PostgresEscapingDialect(),
			check: func(q *Query) bool {
				return q.options.postgresEscapingDialect
			},
		},
		{
			name: "Idiomatic Arrays Option",
			opt:  IdomaticArrays(),
			check: func(q *Query) bool {
				return q.options.idomaticArrays
			},
		},
		{
			name: "With Constants Option",
			opt:  WithConstants(map[string]any{"const": "value"}),
			check: func(q *Query) bool {
				return q.options.constants["const"] == "value"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &Query{options: &Options{}}
			tt.opt(q)
			if !tt.check(q) {
				t.Errorf("Option %s failed to set correct value", tt.name)
			}
		})
	}
}

func TestExecSelect(t *testing.T) {
	tests := []struct {
		name    string
		query   *Query
		input   []any
		want    []any
		wantErr bool
	}{
		{
			name: "Select All Fields",
			query: &Query{
				selectDefinition: sqlparser.SelectExprs{
					&sqlparser.StarExpr{},
				},
			},
			input: []any{
				Map{"id": 1, "name": "test"},
			},
			want: []any{
				Map{"id": 1, "name": "test"},
			},
			wantErr: false,
		},
		{
			name: "Select Specific Fields",
			query: &Query{
				selectDefinition: sqlparser.SelectExprs{
					&sqlparser.AliasedExpr{
						Expr: &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("id")},
					},
				},
			},
			input: []any{
				Map{"id": 1, "name": "test"},
			},
			want: []any{
				Map{"id": 1},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExecSelect(tt.query, tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecSelect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExecSelect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecOrderBy(t *testing.T) {
	tests := []struct {
		name    string
		query   *Query
		input   []any
		want    []any
		wantErr bool
	}{
		{
			name: "Order By Single Field Ascending",
			query: &Query{
				orderByDefinition: []struct {
					Key   string
					Value bool
				}{
					{Key: "id", Value: true},
				},
			},
			input: []any{
				Map{"id": 2.0},
				Map{"id": 1.0},
				Map{"id": 3.0},
			},
			want: []any{
				Map{"id": 1.0},
				Map{"id": 2.0},
				Map{"id": 3.0},
			},
			wantErr: false,
		},
		{
			name: "Order By Single Field Descending",
			query: &Query{
				orderByDefinition: []struct {
					Key   string
					Value bool
				}{
					{Key: "id", Value: false},
				},
			},
			input: []any{
				Map{"id": 2.0},
				Map{"id": 1.0},
				Map{"id": 3.0},
			},
			want: []any{
				Map{"id": 3.0},
				Map{"id": 2.0},
				Map{"id": 1.0},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExecOrderBy(tt.query, tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecOrderBy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExecOrderBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecWhere(t *testing.T) {
	tests := []struct {
		name    string
		query   *Query
		current Map
		want    bool
		wantErr bool
	}{
		{
			name: "Simple Equality Check",
			query: &Query{
				whereDefinition: &sqlparser.Where{
					Type: sqlparser.WhereClause,
					Expr: &sqlparser.ComparisonExpr{
						Left:     &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("id")},
						Right:    sqlparser.NewIntLiteral("1"),
						Operator: sqlparser.EqualOp,
					},
				},
			},
			current: Map{"id": 1},
			want:    true,
			wantErr: false,
		},
		{
			name: "Complex AND Condition",
			query: &Query{
				whereDefinition: &sqlparser.Where{
					Type: sqlparser.WhereClause,
					Expr: &sqlparser.AndExpr{
						Left: &sqlparser.ComparisonExpr{
							Left:     &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("id")},
							Right:    sqlparser.NewIntLiteral("1"),
							Operator: sqlparser.EqualOp,
						},
						Right: &sqlparser.ComparisonExpr{
							Left:     &sqlparser.ColName{Name: sqlparser.NewIdentifierCI("active")},
							Right:    sqlparser.BoolVal(true),
							Operator: sqlparser.EqualOp,
						},
					},
				},
			},
			current: Map{"id": 1, "active": true},
			want:    true,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExecWhere(tt.query, tt.current)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecWhere() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ExecWhere() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExecGroupBy(t *testing.T) {
	tests := []struct {
		name    string
		query   *Query
		input   []any
		want    []any
		wantErr bool
	}{
		{
			name: "Group By Single Field",
			query: &Query{
				groupDefinition: GroupDefinition{
					"category": true,
				},
			},
			input: []any{
				Map{"category": "A", "value": 1},
				Map{"category": "A", "value": 2},
				Map{"category": "B", "value": 3},
			},
			want: []any{
				Map{
					"category": "A",
					"*": []any{
						Map{"category": "A", "value": 1},
						Map{"category": "A", "value": 2},
					},
				},
				Map{
					"category": "B",
					"*": []any{
						Map{"category": "B", "value": 3},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExecGroupBy(tt.query, tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExecGroupBy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExecGroupBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQuery_Exec(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		want    []any
		wantErr bool
	}{
		{
			name:  "Simple Select All",
			query: "SELECT * FROM users",
			data: Map{
				"users": []any{
					Map{"id": 1, "name": "John"},
					Map{"id": 2, "name": "Jane"},
				},
			},
			want: []any{
				Map{"id": 1, "name": "John"},
				Map{"id": 2, "name": "Jane"},
			},
			wantErr: false,
		},
		{
			name:  "Select With Where Clause",
			query: "SELECT * FROM users WHERE id = 1",
			data: Map{
				"users": []any{
					Map{"id": 1, "name": "John"},
					Map{"id": 2, "name": "Jane"},
				},
			},
			want: []any{
				Map{"id": 1, "name": "John"},
			},
			wantErr: false,
		},
		{
			name:  "Select With Order By",
			query: "SELECT * FROM users ORDER BY id DESC",
			data: Map{
				"users": []any{
					Map{"id": 1.0, "name": "John"},
					Map{"id": 2.0, "name": "Jane"},
				},
			},
			want: []any{
				Map{"id": 2.0, "name": "Jane"},
				Map{"id": 1.0, "name": "John"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query)
			if err != nil {
				t.Fatalf("Failed to create query: %v", err)
			}

			got, err := q.Exec()
			if (err != nil) != tt.wantErr {
				t.Errorf("Query.Exec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Query.Exec() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildCte(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		wantErr bool
	}{
		{
			name:  "Simple CTE",
			query: `WITH temp AS (SELECT * FROM test WHERE id = 1) SELECT * FROM temp`,
			data: Map{
				"test": []Map{
					{"id": 1, "name": "test1"},
					{"id": 2, "name": "test2"},
				},
			},
			wantErr: false,
		},
		{
			name: "Multiple CTEs",
			query: `
				WITH temp1 AS (SELECT * FROM test WHERE id = 1),
				     temp2 AS (SELECT * FROM test WHERE id = 2)
				SELECT * FROM temp1 UNION SELECT * FROM temp2`,
			data: Map{
				"test": []Map{
					{"id": 1, "name": "test1"},
					{"id": 2, "name": "test2"},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query, PostgresEscapingDialect())
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildCte() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if err != nil {
					t.Errorf("Exec() error = %v", err)
				}
				if result == nil {
					t.Error("Expected non-nil result")
				}
			}
		})
	}
}

func TestBuildJoin(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		want    []Map
		wantErr bool
	}{
		{
			name: "Inner Join",
			query: `SELECT u.*, o.amount 
					FROM users u 
					JOIN orders o ON u.id = o.user_id`,
			data: Map{
				"users": []Map{
					{"id": 1, "name": "user1"},
					{"id": 2, "name": "user2"},
				},
				"orders": []Map{
					{"id": 1, "user_id": 1, "amount": 100},
					{"id": 2, "user_id": 1, "amount": 200},
				},
			},
			want: []Map{
				{"id": 1, "name": "user1", "amount": 100},
				{"id": 1, "name": "user1", "amount": 200},
			},
			wantErr: false,
		},
		{
			name: "Left Join",
			query: `SELECT u.*, o.amount 
					FROM users u 
					LEFT JOIN orders o ON u.id = o.user_id`,
			data: Map{
				"users": []Map{
					{"id": 1, "name": "user1"},
					{"id": 2, "name": "user2"},
				},
				"orders": []Map{
					{"id": 1, "user_id": 1, "amount": 100},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildJoin() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if err != nil {
					t.Errorf("Exec() error = %v", err)
				}
				if result == nil {
					t.Error("Expected non-nil result")
				}
			}
		})
	}
}

func TestAggregations(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		want    []Map
		wantErr bool
	}{
		{
			name: "Count with Group By",
			query: `SELECT category, COUNT(*) as count 
					FROM test 
					GROUP BY category`,
			data: Map{
				"test": []Map{
					{"category": "A", "value": 1},
					{"category": "A", "value": 2},
					{"category": "B", "value": 3},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query, PostgresEscapingDialect())
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if err != nil {
					t.Errorf("Exec() error = %v", err)
				}
				if result == nil {
					t.Error("Expected non-nil result")
				}
			}
		})
	}
}

func TestExpressions(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		data    Map
		wantErr bool
	}{
		{
			name: "CASE Expression",
			query: `SELECT id, 
					CASE 
						WHEN value > 2 THEN 'High'
						ELSE 'Low'
					END as category
					FROM test`,
			data: Map{
				"test": []Map{
					{"id": 1, "value": 1.0},
					{"id": 2, "value": 3.0},
				},
			},
			wantErr: false,
		},
		{
			name: "Complex WHERE with AND/OR",
			query: `SELECT * FROM test 
					WHERE (value > 1 AND value < 4) 
					OR category = 'A'`,
			data: Map{
				"test": []Map{
					{"id": 1, "value": 2.0, "category": "B"},
					{"id": 2, "value": 5.0, "category": "A"},
				},
			},
			wantErr: false,
		},
		{
			name: "Mathematical Expressions",
			query: `SELECT id, value * 2 as doubled,
					value + 1 as increased
					FROM test`,
			data: Map{
				"test": []Map{
					{"id": 1, "value": 10.0},
					{"id": 2, "value": 20.0},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := New(tt.data, tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				result, err := q.Exec()
				if err != nil {
					t.Errorf("Exec() error = %v", err)
				}
				if result == nil {
					t.Error("Expected non-nil result")
				}
			}
		})
	}
}
