/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.sql;

import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAnalyze extends PlanTestBase {

  //@BeforeClass
  public static void setup() throws Exception {

    test("create table dfs_test.tmp.lineitem as select * from cp.`tpch/lineitem.parquet`");
    test("create table dfs_test.tmp.orders as select * from cp.`tpch/orders.parquet`");
    test("analyze table dfs_test.tmp.lineitem compute statistics for all columns");
    test("analyze table dfs_test.tmp.orders compute statistics for all columns");
  }

  @Test
  public void basic() throws Exception {
    test("alter session set `planner.slice_target` = 1");
    //test("create table dfs_test.tmp.region as select region_id, sales_city, count(*) as cnt from cp.`region.json` group by region_id, sales_city");
    test("create table dfs_test.tmp.region as select * from cp.`region.json`");
    test("analyze table dfs_test.tmp.region compute statistics for all columns");
//    test("analyze table dfs_test.tmp.region compute statistics for all columns");
//    test("analyze table dfs_test.tmp.region compute statistics for all columns");
//    printResult(testRunAndReturn(QueryType.SQL, "SELECT * FROM dfs_test.tmp.region"));
    printResult(testRunAndReturn(QueryType.SQL, "SELECT * FROM dfs_test.tmp.`region/.region.stats.drill`"));
    printResult(testRunAndReturn(QueryType.SQL, "SELECT count(distinct region_id) FROM cp.`region.json`"));
    printResult(testRunAndReturn(QueryType.SQL, "SELECT count(distinct sales_city) FROM cp.`region.json`"));
    printResult(testRunAndReturn(QueryType.SQL, "SELECT hll(sales_region) FROM dfs_test.tmp.`region`"));
  }

  @Test
  public void join() throws Exception {
    //printResult(testRunAndReturn(QueryType.SQL, "SELECT * FROM dfs_test.tmp.lineitem"));
    //printResult(testRunAndReturn(QueryType.SQL, "SELECT * FROM dfs_test.tmp.`lineitem.stats.drill`"));
    //printResult(testRunAndReturn(QueryType.SQL, "SELECT * FROM dfs_test.tmp.orders"));
    //printResult(testRunAndReturn(QueryType.SQL, "SELECT * FROM dfs_test.tmp.`orders.stats.drill`"));

    //printResult(testRunAndReturn(QueryType.SQL, "SELECT count(l_orderkey) from dfs_test.tmp.`lineitem`"));

    //getPlanInString("EXPLAIN PLAN FOR SELECT count(l_orderkey) from dfs_test.tmp.`lineitem`", OPTIQ_FORMAT);
    //getPlanInString("EXPLAIN PLAN FOR SELECT count(distinct l_orderkey) from dfs_test.tmp.`lineitem`", OPTIQ_FORMAT);
    printResult(testRunAndReturn(QueryType.SQL,
        "SELECT * FROM dfs_test.tmp.`lineitem` l join dfs_test.tmp.`orders` o on l.l_orderkey = o.o_orderkey"));
  }
}
