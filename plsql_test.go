// Copyright 2023 Pouya Vedadiyan
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package genql

import (
	"encoding/json"
	"fmt"
	"testing"
)

const _DATASOURCE = `{"data":[{"id":1,"full_name":"Pouya Vedadiyan","email":"vedadiyan@genql.com","profile":{"height":170,"hair":"black"},"likes":[{"type":"game","name":"doom","versions":[1994,1998]},{"type":"game","name":"prince of persia","versions":[1992]},{"type":"movie","name":"star wars","episodes":[1,2,3]}],"technologies":[[{"name":"C","expertiese":"MAX"},{"name":"GO","expertiese":"MAX"}],[{"name":"linux","expertiese":"high"},{"name":"unix","expertiese":"high"}]]},{"id":2,"full_name":"imaginary friend","email": null,"profile":{"height":180,"hair":"blond"},"likes":[{"type":"game","name":"doom","versions":[1994,1998]},{"type":"game","name":"need for speed","versions":[2023]},{"type":"movie","name":"mission impossible","episodes":[1,2,3]}],"technologies":[[{"name":"java","expertiese":"MAX"},{"name":"rust","expertiese":"MAX"}],[{"name":"sql","expertiese":"high"},{"name":"redis","expertiese":"high"}]]}]}`

func ReadDataSource() (map[string]any, error) {
	mapper := make(map[string]any)
	err := json.Unmarshal([]byte(_DATASOURCE), &mapper)
	return mapper, err
}

func Tester(cmd string, expected string, t *testing.T) {
	dataSource, err := ReadDataSource()
	if err != nil {
		t.Log("failed to read initial data source")
		t.FailNow()
	}
	query, err := New(dataSource, cmd, Wrapped(), PostgresEscapingDialect())
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	rs, err := query.Exec()
	v := fmt.Sprintf("%v", rs)
	_ = v
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	array, ok := rs.([]any)
	if !ok {
		t.Logf("test failed. expected array but found %T", rs)
		t.FailNow()
	}
	if len(array) == 0 {
		t.Log("test failed. the output array is empty")
		t.FailNow()
	}
	if fmt.Sprintf("%v", array) != expected {
		t.FailNow()
	}
}

func TestSelectStar(t *testing.T) {
	dataSource, err := ReadDataSource()
	if err != nil {
		t.Log("failed to read initial data source")
		t.FailNow()
	}
	cmd := `SELECT * FROM "root.data"`
	Tester(cmd, fmt.Sprintf("%v", dataSource["data"]), t)
}

func TestSelect(t *testing.T) {
	expected := "[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3] map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]"
	cmd := `SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes FROM "root.data"`
	Tester(cmd, expected, t)
}

func TestCte(t *testing.T) {
	expected := "[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3] map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]"
	cmd := `WITH Main AS (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes FROM "root.data") SELECT * FROM Main`
	Tester(cmd, expected, t)
}

func TestCteWithIndexSelector(t *testing.T) {
	expected := "[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]"
	cmd := `WITH Main AS (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes FROM "root.data") SELECT * FROM "Main[0]"`
	Tester(cmd, expected, t)
}

func TestCteWithKeySelector(t *testing.T) {
	expected := "[[map[name:doom type:game versions:[1994 1998]] map[name:prince of persia type:game versions:[1992]] map[episodes:[1 2 3] name:star wars type:movie]] [map[name:doom type:game versions:[1994 1998]] map[name:need for speed type:game versions:[2023]] map[episodes:[1 2 3] name:mission impossible type:movie]]]"
	cmd := `WITH Main AS (SELECT likes FROM "root.data") SELECT * FROM Main.likes`
	Tester(cmd, expected, t)
}

func TestCteWithPipeSelector(t *testing.T) {
	expected := "[[map[name:doom] map[name:prince of persia] map[name:star wars]] [map[name:doom] map[name:need for speed] map[name:mission impossible]]]"
	cmd := `WITH Main AS (SELECT likes FROM "root.data") SELECT * FROM "Main.likes{name}"`
	Tester(cmd, expected, t)
}
func TestCteWithDimensionSelector(t *testing.T) {
	expected := "[map[expertiese:MAX name:GO] map[expertiese:MAX name:rust]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT * FROM "Main.technologies[0:1]"`
	Tester(cmd, expected, t)
}

func TestSubQuery(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]] map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main`
	Tester(cmd, expected, t)
}

func TestWhere(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where id = 1`
	Tester(cmd, expected, t)
}

func TestBinaryAnd(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where id = 1 AND full_name = 'Pouya Vedadiyan'`
	Tester(cmd, expected, t)
}

func TestBinaryOr(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]] map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where id = 1 OR full_name = 'imaginary friend'`
	Tester(cmd, expected, t)
}

func TestBinaryMinus(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]] map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where id = 2-1 OR full_name = 'imaginary friend'`
	Tester(cmd, expected, t)
}

func TestBinaryPlus(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]] map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where id = 1+1-1 OR full_name = 'imaginary friend'`
	Tester(cmd, expected, t)
}

func TestBinaryMultiply(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]] map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where id = 1*1 OR full_name = 'imaginary friend'`
	Tester(cmd, expected, t)
}

func TestBinaryDivide(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]] map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where id = 1/1 OR full_name = 'imaginary friend'`
	Tester(cmd, expected, t)
}

func TestIs(t *testing.T) {
	expected := "[map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where email IS NULL`
	Tester(cmd, expected, t)
}

func TestIsNot(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]] map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where id IS NOT NULL`
	Tester(cmd, expected, t)
}

func TestIn(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]] map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where id IN (1,2)`
	Tester(cmd, expected, t)
}

func TestNotIn(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]] map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where id NOT IN (10,20)`
	Tester(cmd, expected, t)
}

func TestGt(t *testing.T) {
	expected := "[map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where profile.height > 170`
	Tester(cmd, expected, t)
}

func TestGte(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]] map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where profile.height >= 170`
	Tester(cmd, expected, t)
}

func TestLt(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where profile.height < 180`
	Tester(cmd, expected, t)
}

func TestLte(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]] map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where profile.height <= 180`
	Tester(cmd, expected, t)
}

func TestBetween(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]] map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where profile.height BETWEEN 169 AND 181`
	Tester(cmd, expected, t)
}

func TestNotBetween(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]] map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where profile.height NOT BETWEEN 181 AND 191`
	Tester(cmd, expected, t)
}

func TestLike(t *testing.T) {
	expected := "[map[data:[map[full_name:Pouya Vedadiyan harcoded_value:HARD CODED id:1 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where email LIKE '%@%'`
	Tester(cmd, expected, t)
}

func TestNotLike(t *testing.T) {
	expected := "[map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where email NOT LIKE '%@%'`
	Tester(cmd, expected, t)
}

func TestNotEqual(t *testing.T) {
	expected := "[map[data:[map[full_name:imaginary friend harcoded_value:HARD CODED id:2 liked:doom total_likes:3]]]]"
	cmd := `WITH Main AS (SELECT * FROM "root.data") SELECT (SELECT id, full_name, 'HARD CODED' AS harcoded_value, "likes[0].name" AS liked, SCOPED.COUNT(likes) AS total_likes) AS data FROM Main where id != 1`
	Tester(cmd, expected, t)
}
func TestSubString(t *testing.T) {
	expected := "[map[OK:Sub]]"
	cmd := `SELECT SUBSTRING('Test Substring Function', 5, 3) AS OK FROM "root.data" LIMIT 1`
	Tester(cmd, expected, t)
}

func TestUnary(t *testing.T) {
	expected := "[map[OK:false]]"
	cmd := `SELECT !TRUE AS OK FROM "root.data" LIMIT 1`
	Tester(cmd, expected, t)
}

func TestLimit(t *testing.T) {
	expected := "[map[id:1]]"
	cmd := `SELECT id FROM "root.data" LIMIT 1 `
	Tester(cmd, expected, t)
}

func TestOffset(t *testing.T) {
	expected := "[map[id:2]]"
	cmd := `SELECT id FROM "root.data" LIMIT 1 OFFSET 1 `
	Tester(cmd, expected, t)
}

func TestOrderByDesc(t *testing.T) {
	expected := "[map[episodes:[1 2 3] name:star wars type:movie] map[episodes:[1 2 3] name:mission impossible type:movie] map[name:prince of persia type:game versions:[1992]] map[name:need for speed type:game versions:[2023]] map[name:doom type:game versions:[1994 1998]] map[name:doom type:game versions:[1994 1998]]]"
	cmd := `SELECT * FROM "mix=>root.data.likes" ORDER BY type desc, name desc`
	Tester(cmd, expected, t)
}

func TestOrderByAcs(t *testing.T) {
	expected := "[map[episodes:[1 2 3] name:mission impossible type:movie] map[episodes:[1 2 3] name:star wars type:movie] map[name:doom type:game versions:[1994 1998]] map[name:doom type:game versions:[1994 1998]] map[name:need for speed type:game versions:[2023]] map[name:prince of persia type:game versions:[1992]]]"
	cmd := `SELECT * FROM "mix=>root.data.likes" ORDER BY type desc, name asc`
	Tester(cmd, expected, t)
}

func TestGroupBy(t *testing.T) {
	expected := "[map[type:game] map[type:movie]]"
	cmd := `SELECT type FROM "mix=>root.data.likes" GROUP BY type ORDER BY type`
	Tester(cmd, expected, t)
}

func TestAggregatedFunction(t *testing.T) {
	expected := "[map[number:2] map[number:2]]"
	cmd := `SELECT COUNT(type) AS number FROM "mix=>root.data.likes" GROUP BY type`
	Tester(cmd, expected, t)
}

func TestDistinct(t *testing.T) {
	expected := "[map[name:doom type:game versions:[1994 1998]] map[name:prince of persia type:game versions:[1992]] map[episodes:[1 2 3] name:star wars type:movie] map[name:need for speed type:game versions:[2023]] map[episodes:[1 2 3] name:mission impossible type:movie]]"
	cmd := `SELECT DISTINCT * FROM "mix=>root.data.likes"`
	Tester(cmd, expected, t)
}

func TestInnerJoin(t *testing.T) {
	expected := "[map[A_full_name:Pouya Vedadiyan B_full_name:Pouya Vedadiyan] map[A_full_name:imaginary friend B_full_name:imaginary friend]]"
	cmd := `SELECT A.full_name AS A_full_name , B.full_name AS B_full_name FROM root.data A JOIN root.data B on A.id = B.id`
	Tester(cmd, expected, t)
}

func TestLeftJoin(t *testing.T) {
	expected := "[map[A_name:doom B_name:<nil>] map[A_name:prince of persia B_name:<nil>] map[A_name:star wars B_name:<nil>] map[A_name:doom B_name:<nil>] map[A_name:need for speed B_name:<nil>] map[A_name:mission impossible B_name:<nil>]]"
	cmd := `SELECT A.name AS A_name, B.name AS B_name FROM "mix=>root.data.likes" A LEFT JOIN "mix=>root.data.technologies" B on A.name = B.name`
	Tester(cmd, expected, t)
}

func TestRightJoin(t *testing.T) {
	expected := "[map[A_name:<nil> B_name:C] map[A_name:<nil> B_name:GO] map[A_name:<nil> B_name:linux] map[A_name:<nil> B_name:unix] map[A_name:<nil> B_name:java] map[A_name:<nil> B_name:rust] map[A_name:<nil> B_name:sql] map[A_name:<nil> B_name:redis]]"
	cmd := `SELECT A.name AS A_name, B.name AS B_name FROM "mix=>root.data.likes" A RIGHT JOIN "mix=>root.data.technologies" B on A.name = B.name`
	Tester(cmd, expected, t)
}

func TestFuse(t *testing.T) {
	expected := "[map[id:1 test.hair:black test.height:170] map[id:2 test.hair:blond test.height:180]]"
	cmd := `SELECT id, FUSE(profile) AS test FROM "root.data"`
	Tester(cmd, expected, t)
}
