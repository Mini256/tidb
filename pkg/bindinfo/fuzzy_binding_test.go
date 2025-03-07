// Copyright 2023 PingCAP, Inc.
//
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

package bindinfo_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

// for testing, only returns Original_sql, Bind_sql, Default_db, Status, Source, Type, Sql_digest
func showBinding(tk *testkit.TestKit, showStmt string) [][]interface{} {
	rows := tk.MustQuery(showStmt).Sort().Rows()
	result := make([][]interface{}, len(rows))
	for i, r := range rows {
		result[i] = append(result[i], r[:4]...)
		result[i] = append(result[i], r[8:10]...)
	}
	return result
}

func removeAllBindings(tk *testkit.TestKit, global bool) {
	scope := "session"
	if global {
		scope = "global"
	}
	res := showBinding(tk, fmt.Sprintf("show %v bindings", scope))
	for _, r := range res {
		if r[4] == "builtin" {
			continue
		}
		tk.MustExec(fmt.Sprintf("drop %v binding for sql digest '%v'", scope, r[5]))
	}
	tk.MustQuery(fmt.Sprintf("show %v bindings", scope)).Check(testkit.Rows()) // empty
}

func TestFuzzyBindingBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)

	tk1.MustExec(`use test`)
	tk1.MustExec(`create table t (a int, b int, c int, d int, e int, key(a), key(b), key(c), key(d), key(e))`)
	tk1.MustExec(`create database test1`)
	tk1.MustExec(`use test1`)
	tk1.MustExec(`create table t (a int, b int, c int, d int, e int, key(a), key(b), key(c), key(d), key(e))`)
	tk1.MustExec(`create database test2`)
	tk1.MustExec(`use test2`)
	tk1.MustExec(`create table t (a int, b int, c int, d int, e int, key(a), key(b), key(c), key(d), key(e))`)

	for _, scope := range []string{"", "session", "global"} {
		tk := testkit.NewTestKit(t, store)
		for _, idx := range []string{"a", "b", "c", "d", "e"} {
			tk.MustExec("use test")
			tk.MustExec(fmt.Sprintf(`create %v binding using select /*+ use_index(t, %v) */ * from *.t`, scope, idx))
			for _, useDB := range []string{"test", "test1", "test2"} {
				tk.MustExec("use " + useDB)
				for _, testDB := range []string{"", "test.", "test1.", "test2."} {
					tk.MustExec(`set @@tidb_opt_enable_fuzzy_binding=1`) // enabled
					require.True(t, tk.MustUseIndex(fmt.Sprintf("select * from %vt", testDB), idx))
					tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
					require.True(t, tk.MustUseIndex(fmt.Sprintf("select * from %vt", testDB), idx))
					tk.MustQuery(`show warnings`).Check(testkit.Rows())  // no warning
					tk.MustExec(`set @@tidb_opt_enable_fuzzy_binding=0`) // disabled
					tk.MustQuery(fmt.Sprintf("select * from %vt", testDB))
					tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))
				}
			}
		}
	}
}

func TestFuzzyDuplicatedBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int, e int, key(a), key(b), key(c), key(d), key(e))`)

	tk.MustExec(`create global binding using select * from *.t`)
	require.Equal(t, showBinding(tk, "show global bindings"),
		[][]interface{}{{"select * from `*` . `t`", "SELECT * FROM `*`.`t`", "", "enabled", "manual", "a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013"}})

	// if duplicated, the old one will be replaced
	tk.MustExec(`create global binding using select /*+ use_index(t, a) */ * from *.t`)
	require.Equal(t, showBinding(tk, "show global bindings"),
		[][]interface{}{{"select * from `*` . `t`", "SELECT /*+ use_index(`t` `a`)*/ * FROM `*`.`t`", "", "enabled", "manual", "a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013"}})

	// if duplicated, the old one will be replaced
	tk.MustExec(`create global binding using select /*+ use_index(t, b) */ * from *.t`)
	require.Equal(t, showBinding(tk, "show global bindings"),
		[][]interface{}{{"select * from `*` . `t`", "SELECT /*+ use_index(`t` `b`)*/ * FROM `*`.`t`", "", "enabled", "manual", "a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013"}})
}

func TestFuzzyBindingPriority(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`set @@tidb_opt_enable_fuzzy_binding=1`)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int)`)
	tk.MustExec(`create table t2 (a int)`)
	tk.MustExec(`create table t3 (a int)`)
	tk.MustExec(`create table t4 (a int)`)
	tk.MustExec(`create table t5 (a int)`)

	// The less wildcard number, the higher priority.
	tk.MustExec(`create global binding using select /*+ leading(t1, t2, t3, t4, t5) */ * from *.t1, *.t2, *.t3, *.t4, *.t5`)
	tk.MustExec(`explain format='verbose' select * from t1, t2, t3, t4, t5`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Note 1105 Using the bindSQL: SELECT /*+ leading(`t1`, `t2`, `t3`, `t4`, `t5`)*/ * FROM ((((`*`.`t1`) JOIN `*`.`t2`) JOIN `*`.`t3`) JOIN `*`.`t4`) JOIN `*`.`t5`"))

	tk.MustExec(`create global binding using select /*+ leading(t1, t2, t3, t4, t5) */ * from *.t1, *.t2, *.t3, *.t4, t5`)
	tk.MustExec(`explain format='verbose' select * from t1, t2, t3, t4, t5`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Note 1105 Using the bindSQL: SELECT /*+ leading(`t1`, `t2`, `t3`, `t4`, `t5`)*/ * FROM ((((`*`.`t1`) JOIN `*`.`t2`) JOIN `*`.`t3`) JOIN `*`.`t4`) JOIN `test`.`t5`"))

	tk.MustExec(`create global binding using select /*+ leading(t1, t2, t3, t4, t5) */ * from *.t1, *.t2, *.t3, t4, t5`)
	tk.MustExec(`explain format='verbose' select * from t1, t2, t3, t4, t5`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Note 1105 Using the bindSQL: SELECT /*+ leading(`t1`, `t2`, `t3`, `t4`, `t5`)*/ * FROM ((((`*`.`t1`) JOIN `*`.`t2`) JOIN `*`.`t3`) JOIN `test`.`t4`) JOIN `test`.`t5`"))

	tk.MustExec(`create global binding using select /*+ leading(t1, t2, t3, t4, t5) */ * from *.t1, *.t2, t3, t4, t5`)
	tk.MustExec(`explain format='verbose' select * from t1, t2, t3, t4, t5`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Note 1105 Using the bindSQL: SELECT /*+ leading(`t1`, `t2`, `t3`, `t4`, `t5`)*/ * FROM ((((`*`.`t1`) JOIN `*`.`t2`) JOIN `test`.`t3`) JOIN `test`.`t4`) JOIN `test`.`t5`"))

	tk.MustExec(`create global binding using select /*+ leading(t1, t2, t3, t4, t5) */ * from *.t1, t2, t3, t4, t5`)
	tk.MustExec(`explain format='verbose' select * from t1, t2, t3, t4, t5`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Note 1105 Using the bindSQL: SELECT /*+ leading(`t1`, `t2`, `t3`, `t4`, `t5`)*/ * FROM ((((`*`.`t1`) JOIN `test`.`t2`) JOIN `test`.`t3`) JOIN `test`.`t4`) JOIN `test`.`t5`"))

	tk.MustExec(`create global binding using select /*+ leading(t1, t2, t3, t4, t5) */ * from t1, t2, t3, t4, t5`)
	tk.MustExec(`explain format='verbose' select * from t1, t2, t3, t4, t5`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Note 1105 Using the bindSQL: SELECT /*+ leading(`t1`, `t2`, `t3`, `t4`, `t5`)*/ * FROM ((((`test`.`t1`) JOIN `test`.`t2`) JOIN `test`.`t3`) JOIN `test`.`t4`) JOIN `test`.`t5`"))

	// Session binding's priority is higher than global binding's.
	tk.MustExec(`create session binding using select /*+ leading(t1, t2, t3, t4, t5) */ * from *.t1, *.t2, *.t3, *.t4, *.t5`)
	tk.MustExec(`explain format='verbose' select * from t1, t2, t3, t4, t5`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Note 1105 Using the bindSQL: SELECT /*+ leading(`t1`, `t2`, `t3`, `t4`, `t5`)*/ * FROM ((((`*`.`t1`) JOIN `*`.`t2`) JOIN `*`.`t3`) JOIN `*`.`t4`) JOIN `*`.`t5`"))
}

func TestCreateUpdateFuzzyBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int)`)

	// drop/show/update binding for sql digest can work for global universal bindings
	tk.MustExec(`create global binding using select * from *.t`)
	require.Equal(t, showBinding(tk, "show global bindings"),
		[][]interface{}{{"select * from `*` . `t`", "SELECT * FROM `*`.`t`", "", "enabled", "manual", "a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013"}})
	require.Equal(t, showBinding(tk, "show global bindings"), [][]interface{}{
		{"select * from `*` . `t`", "SELECT * FROM `*`.`t`", "", "enabled", "manual", "a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013"}})
	tk.MustExec(`set binding disabled for sql digest 'a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013'`)
	require.Equal(t, showBinding(tk, "show global bindings"), [][]interface{}{
		{"select * from `*` . `t`", "SELECT * FROM `*`.`t`", "", "disabled", "manual", "a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013"}})
	tk.MustExec(`set binding enabled for sql digest 'a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013'`)
	require.Equal(t, showBinding(tk, "show global bindings"), [][]interface{}{
		{"select * from `*` . `t`", "SELECT * FROM `*`.`t`", "", "enabled", "manual", "a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013"}})
	tk.MustExec(`drop global binding for sql digest 'a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013'`)
	require.Equal(t, showBinding(tk, "show global bindings"), [][]interface{}{})

	// drop/show/update binding for sql digest can work for session universal bindings
	tk.MustExec(`create session binding using select * from *.t`)
	require.Equal(t, showBinding(tk, "show session bindings"),
		[][]interface{}{{"select * from `*` . `t`", "SELECT * FROM `*`.`t`", "", "enabled", "manual", "a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013"}})
	require.Equal(t, showBinding(tk, "show session bindings"), [][]interface{}{
		{"select * from `*` . `t`", "SELECT * FROM `*`.`t`", "", "enabled", "manual", "a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013"}})
	tk.MustExec(`drop session binding for sql digest 'a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013'`)
	require.Equal(t, showBinding(tk, "show session bindings"), [][]interface{}{})
}

func TestFuzzyBindingSwitch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)

	tk1.MustExec(`use test`)
	tk1.MustExec(`create table t (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
	tk1.MustExec(`create database test1`)

	// switch can work for both global and session universal bindings
	// test for session bindings
	tk1.MustExec(`create session binding using select /*+ use_index(t, b) */ * from *.t`)
	tk1.MustExec(`use test1`)
	tk1.MustQuery(`select * from test.t`).Check(testkit.Rows())
	tk1.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))
	tk1.MustExec(`set @@tidb_opt_enable_fuzzy_binding=1`)
	tk1.MustUseIndex(`select * from test.t`, "b")
	tk1.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
	tk1.MustExec(`set @@tidb_opt_enable_fuzzy_binding=0`)
	tk1.MustQuery(`select * from test.t`).Check(testkit.Rows())
	tk1.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))

	// test for global bindings
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(`use test1`)
	tk2.MustExec(`create global binding using select /*+ use_index(t, b) */ * from *.t`)
	tk2.MustQuery(`select * from test.t`).Check(testkit.Rows())
	tk2.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))
	tk2.MustExec(`set @@tidb_opt_enable_fuzzy_binding=1`)
	tk2.MustUseIndex(`select * from test.t`, "b")
	tk2.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
	tk2.MustExec(`set @@tidb_opt_enable_fuzzy_binding=0`)
	tk2.MustQuery(`select * from test.t`).Check(testkit.Rows())
	tk2.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))

	// the default value is off
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustQuery(`select @@tidb_opt_enable_fuzzy_binding`).Check(testkit.Rows("0"))
	tk3.MustQuery(`show session variables like 'tidb_opt_enable_fuzzy_binding'`).Check(testkit.Rows("tidb_opt_enable_fuzzy_binding OFF"))
	tk3.MustQuery(`show global variables like 'tidb_opt_enable_fuzzy_binding'`).Check(testkit.Rows("tidb_opt_enable_fuzzy_binding OFF"))
}

func TestFuzzyBindingSetVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, key(a), key(b))`)
	tk.MustExec(`create global binding using select /*+ use_index(t, a) */ * from *.t`)

	tk.MustExec(`set @@tidb_opt_enable_fuzzy_binding=0`)
	tk.MustExec(`select * from t`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))
	tk.MustExec(`select /*+ set_var(tidb_opt_enable_fuzzy_binding=1) */ * from t`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
	tk.MustExec(`select /*+ set_var(tidb_opt_enable_fuzzy_binding=0) */ * from t`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))

	tk.MustExec(`set @@tidb_opt_enable_fuzzy_binding=1`)
	tk.MustExec(`select * from t`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
	tk.MustExec(`select /*+ set_var(tidb_opt_enable_fuzzy_binding=0) */ * from t`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("0"))
	tk.MustExec(`select /*+ set_var(tidb_opt_enable_fuzzy_binding=1) */ * from t`)
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
}

func TestFuzzyBindingGC(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)

	tk.MustExec(`create global binding using select /*+ use_index(t, b) */ * from *.t`)
	require.Equal(t, showBinding(tk, "show global bindings"),
		[][]interface{}{{"select * from `*` . `t`", "SELECT /*+ use_index(`t` `b`)*/ * FROM `*`.`t`", "", "enabled", "manual", "a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013"}})
	tk.MustExec(`drop global binding for sql digest 'a17da0a38af0f1d75229c5cd064d5222a610c5e5ef59436be5da1564c16f1013'`)
	require.Equal(t, showBinding(tk, "show global bindings"), [][]interface{}{}) // empty
	tk.MustQuery(`select bind_sql, status from mysql.bind_info where source != 'builtin'`).Check(
		testkit.Rows("SELECT /*+ use_index(`t` `b`)*/ * FROM `*`.`t` deleted")) // status=deleted

	updateTime := time.Now().Add(-(15 * bindinfo.Lease))
	updateTimeStr := types.NewTime(types.FromGoTime(updateTime), mysql.TypeTimestamp, 3).String()
	tk.MustExec(fmt.Sprintf("update mysql.bind_info set update_time = '%v' where source != 'builtin'", updateTimeStr))
	bindHandle := bindinfo.NewGlobalBindingHandle(&mockSessionPool{tk.Session()})
	require.NoError(t, bindHandle.GCGlobalBinding())
	tk.MustQuery(`select bind_sql, status from mysql.bind_info where source != 'builtin'`).Check(testkit.Rows()) // empty after GC
}

func TestFuzzyBindingInList(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database test1`)
	tk.MustExec(`use test1`)
	tk.MustExec(`create table t1 (a int)`)
	tk.MustExec(`create table t2 (a int)`)
	tk.MustExec(`create database test2`)
	tk.MustExec(`use test2`)
	tk.MustExec(`create table t1 (a int)`)
	tk.MustExec(`create table t2 (a int)`)

	tk.MustExec(`use test`)
	tk.MustExec(`set @@tidb_opt_enable_fuzzy_binding=1`)
	tk.MustExec(`create global binding using select * from *.t1 where a in (1,2,3)`)
	tk.MustExec(`explain format='verbose' select * from test1.t1 where a in (1)`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Note 1105 Using the bindSQL: SELECT * FROM `*`.`t1` WHERE `a` IN (1,2,3)"))
	tk.MustExec(`explain format='verbose' select * from test2.t1 where a in (1,2,3,4,5)`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Note 1105 Using the bindSQL: SELECT * FROM `*`.`t1` WHERE `a` IN (1,2,3)"))
	tk.MustExec(`use test1`)
	tk.MustExec(`explain format='verbose' select * from t1 where a in (1)`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Note 1105 Using the bindSQL: SELECT * FROM `*`.`t1` WHERE `a` IN (1,2,3)"))

	tk.MustExec(`create global binding using select * from *.t1, *.t2 where t1.a in (1) and t2.a in (2)`)
	for _, currentDB := range []string{"test1", "test2"} {
		for _, t1DB := range []string{"", "test1.", "test2."} {
			for _, t2DB := range []string{"", "test1.", "test2."} {
				for _, t1Cond := range []string{"(1)", "(1,2,3)", "(1,1,1,1,1,1,2,2,2)"} {
					for _, t2Cond := range []string{"(1)", "(1,2,3)", "(1,1,1,1,1,1,2,2,2)"} {
						tk.MustExec(`use ` + currentDB)
						sql := fmt.Sprintf(`explain format='verbose' select * from %st1, %st2 where t1.a in %s and t2.a in %s`, t1DB, t2DB, t1Cond, t2Cond)
						tk.MustExec(sql)
						tk.MustQuery(`show warnings`).Check(testkit.Rows("Note 1105 Using the bindSQL: SELECT * FROM (`*`.`t1`) JOIN `*`.`t2` WHERE `t1`.`a` IN (1) AND `t2`.`a` IN (2)"))
					}
				}
			}
		}
	}
}

func TestFuzzyBindingPlanCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`set @@tidb_opt_enable_fuzzy_binding=1`)
	tk.MustExec(`create table t (a int, b int, c int, d int, e int, key(a), key(b), key(c), key(d))`)

	hasPlan := func(operator, accessInfo string) {
		tkProcess := tk.Session().ShowProcess()
		ps := []*util.ProcessInfo{tkProcess}
		tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
		flag := false
		for _, row := range rows {
			op := row[0].(string)
			info := row[4].(string)
			if strings.Contains(op, operator) && strings.Contains(info, accessInfo) {
				flag = true
				break
			}
		}
		require.Equal(t, flag, true)
	}

	tk.MustExec(`prepare stmt from 'select * from t where e > ?'`)
	tk.MustExec(`set @v=0`)
	tk.MustExec(`execute stmt using @v`)
	hasPlan("TableFullScan", "")

	tk.MustExec(`create database test2`)
	tk.MustExec(`use test2`)
	tk.MustExec(`create global binding using select /*+ use_index(t, a) */ * from *.t where e > 1`)
	tk.MustExec(`execute stmt using @v`)
	hasPlan("IndexFullScan", "index:a(a)")

	tk.MustExec(`create global binding using select /*+ use_index(t, b) */ * from *.t where e > 1`)
	tk.MustExec(`execute stmt using @v`)
	hasPlan("IndexFullScan", "index:b(b)")

	tk.MustExec(`create global binding using select /*+ use_index(t, c) */ * from *.t where e > 1`)
	tk.MustExec(`execute stmt using @v`)
	hasPlan("IndexFullScan", "index:c(c)")
}

func TestFuzzyBindingHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	for _, db := range []string{"db1", "db2", "db3"} {
		tk.MustExec(`create database ` + db)
		tk.MustExec(`use ` + db)
		tk.MustExec(`create table t1 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
		tk.MustExec(`create table t2 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
		tk.MustExec(`create table t3 (a int, b int, c int, d int, key(a), key(b), key(c), key(d))`)
	}
	tk.MustExec(`set @@tidb_opt_enable_fuzzy_binding=1`)

	for _, c := range []struct {
		binding   string
		qTemplate string
	}{
		// use index
		{`create global binding using select /*+ use_index(t1, c) */ * from *.t1 where a=1`,
			`select * from %st1 where a=1000`},
		{`create global binding using select /*+ use_index(t1, c) */ * from *.t1 where d<1`,
			`select * from %st1 where d<10000`},
		{`create global binding using select /*+ use_index(t1, c) */ * from *.t1, *.t2 where t1.d<1`,
			`select * from %st1, t2 where t1.d<100`},
		{`create global binding using select /*+ use_index(t1, c) */ * from *.t1, *.t2 where t1.d<1`,
			`select * from t1, %st2 where t1.d<100`},
		{`create global binding using select /*+ use_index(t1, c), use_index(t2, a) */ * from *.t1, *.t2 where t1.d<1`,
			`select * from %st1, t2 where t1.d<100`},
		{`create global binding using select /*+ use_index(t1, c), use_index(t2, a) */ * from *.t1, *.t2 where t1.d<1`,
			`select * from t1, %st2 where t1.d<100`},
		{`create global binding using select /*+ use_index(t1, c), use_index(t2, a) */ * from *.t1, *.t2, *.t3 where t1.d<1`,
			`select * from %st1, t2, t3 where t1.d<100`},
		{`create global binding using select /*+ use_index(t1, c), use_index(t2, a) */ * from *.t1, *.t2, *.t3 where t1.d<1`,
			`select * from t1, t2, %st3 where t1.d<100`},

		// ignore index
		{`create global binding using select /*+ ignore_index(t1, b) */ * from *.t1 where b=1`,
			`select * from %st1 where b=1000`},
		{`create global binding using select /*+ ignore_index(t1, b) */ * from *.t1 where b>1`,
			`select * from %st1 where b>1000`},
		{`create global binding using select /*+ ignore_index(t1, b) */ * from *.t1 where b in (1,2)`,
			`select * from %st1 where b in (1)`},
		{`create global binding using select /*+ ignore_index(t1, b) */ * from *.t1 where b in (1,2)`,
			`select * from %st1 where b in (1,2,3,4,5)`},

		// order index hint
		{`create global binding using select /*+ order_index(t1, a) */ a from *.t1 where a<10 order by a limit 10`,
			`select a from %st1 where a<10000 order by a limit 10`},
		{`create global binding using select /*+ order_index(t1, b) */ b from *.t1 where b>10 order by b limit 1111`,
			`select b from %st1 where b>2 order by b limit 10`},

		// no order index hint
		{`create global binding using select /*+ no_order_index(t1, c) */ c from *.t1 where c<10 order by c limit 10`,
			`select c from %st1 where c<10000 order by c limit 10`},
		{`create global binding using select /*+ no_order_index(t1, d) */ d from *.t1 where d>10 order by d limit 1111`,
			`select d from %st1 where d>2 order by d limit 10`},

		// agg hint
		{`create global binding using select /*+ hash_agg() */ count(*) from *.t1 group by a`,
			`select count(*) from %st1 group by a`},
		{`create global binding using select /*+ stream_agg() */ count(*) from *.t1 group by b`,
			`select count(*) from %st1 group by b`},

		// to_cop hint
		{`create global binding using select /*+ agg_to_cop() */ sum(a) from *.t1`,
			`select sum(a) from %st1`},
		{`create global binding using select /*+ limit_to_cop() */ a from *.t1 limit 10`,
			`select a from %st1 limit 101`},

		// index merge hint
		{`create global binding using select /*+ use_index_merge(t1, c, d) */ * from *.t1 where c=1 or d=1`,
			`select * from %st1 where c=1000 or d=1000`},
		{`create global binding using select /*+ no_index_merge() */ * from *.t1 where a=1 or b=1`,
			`select * from %st1 where a=1000 or b=1000`},

		// join type hint
		{`create global binding using select /*+ hash_join(t1) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from %st1, t2 where t1.a=t2.a`},
		{`create global binding using select /*+ hash_join(t2) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create global binding using select /*+ hash_join(t2) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create global binding using select /*+ hash_join_build(t1) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create global binding using select /*+ hash_join_probe(t1) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create global binding using select /*+ merge_join(t1) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from %st1, t2 where t1.a=t2.a`},
		{`create global binding using select /*+ merge_join(t2) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create global binding using select /*+ merge_join(t2) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create global binding using select /*+ inl_join(t1) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from %st1, t2 where t1.a=t2.a`},
		{`create global binding using select /*+ inl_join(t2) */ * from *.t1, *.t2 where t1.a=t2.a`,
			`select * from t1, %st2 where t1.a=t2.a`},
		{`create global binding using select /*+ inl_join(t2) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},

		// no join type hint
		{`create global binding using select /*+ no_hash_join(t1) */ * from *.t1, *.t2 where t1.b=t2.b`,
			`select * from %st1, t2 where t1.b=t2.b`},
		{`create global binding using select /*+ no_hash_join(t2) */ * from *.t1, *.t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create global binding using select /*+ no_hash_join(t2) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create global binding using select /*+ no_merge_join(t1) */ * from *.t1, *.t2 where t1.b=t2.b`,
			`select * from %st1, t2 where t1.b=t2.b`},
		{`create global binding using select /*+ no_merge_join(t2) */ * from *.t1, *.t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create global binding using select /*+ no_merge_join(t2) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create global binding using select /*+ no_index_join(t1) */ * from *.t1, *.t2 where t1.b=t2.b`,
			`select * from %st1, t2 where t1.b=t2.b`},
		{`create global binding using select /*+ no_index_join(t2) */ * from *.t1, *.t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create global binding using select /*+ no_index_join(t2) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},

		// join order hint
		{`create global binding using select /*+ leading(t2) */ * from *.t1, *.t2 where t1.b=t2.b`,
			`select * from %st1, t2 where t1.b=t2.b`},
		{`create global binding using select /*+ leading(t2) */ * from *.t1, *.t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create global binding using select /*+ leading(t2, t1) */ * from *.t1, *.t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create global binding using select /*+ leading(t1, t2) */ * from *.t1, *.t2 where t1.c=t2.c`,
			`select * from t1, %st2 where t1.c=t2.c`},
		{`create global binding using select /*+ leading(t1) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create global binding using select /*+ leading(t2) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create global binding using select /*+ leading(t2,t3) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
		{`create global binding using select /*+ leading(t2,t3,t1) */ * from *.t1, *.t2, *.t3 where t1.a=t2.a and t3.b=t2.b`,
			`select * from t1, %st2, t3 where t1.a=t2.a and t3.b=t2.b`},
	} {
		removeAllBindings(tk, true)
		tk.MustExec(c.binding)
		for _, currentDB := range []string{"db1", "db2", "db3"} {
			tk.MustExec(`use ` + currentDB)
			for _, db := range []string{"db1.", "db2.", "db3.", ""} {
				query := fmt.Sprintf(c.qTemplate, db)
				tk.MustExec(query)
				tk.MustQuery(`show warnings`).Check(testkit.Rows()) // no warning
				tk.MustExec(query)
				tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
			}
		}
	}
}
