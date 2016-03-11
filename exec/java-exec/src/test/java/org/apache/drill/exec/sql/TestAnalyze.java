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
import org.apache.drill.exec.ExecConstants;
import org.junit.Test;

public class TestAnalyze extends PlanTestBase {

  // Analyze for all columns
  @Test
  public void basic1() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("CREATE TABLE dfs_test.tmp.region_basic1 AS SELECT * from cp.`region.json`");
      test("ANALYZE TABLE dfs_test.tmp.region_basic1 COMPUTE STATISTICS FOR ALL COLUMNS");
      test("SELECT * FROM dfs_test.tmp.`region_basic1/.stats.drill`");
      testBuilder()
          .sqlQuery("SELECT count(*) as cnt FROM dfs_test.tmp.`region_basic1/.stats.drill`")
          .unOrdered()
          .baselineColumns("cnt")
          .baselineValues(7L)
          .go();
    } finally {
      test("ALTER SESSION SET `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }

  // Analyze for for only part of the columns in table
  @Test
  public void basic2() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("CREATE TABLE dfs_test.tmp.region_basic2 AS SELECT * from cp.`region.json`");
      test("ANALYZE TABLE dfs_test.tmp.region_basic2 COMPUTE STATISTICS FOR COLUMNS (region_id, sales_city)");
      test("SELECT * FROM dfs_test.tmp.`region_basic2/.stats.drill`");
      testBuilder()
          .sqlQuery("SELECT count(*) as cnt FROM dfs_test.tmp.`region_basic2/.stats.drill`")
          .unOrdered()
          .baselineColumns("cnt")
          .baselineValues(2L)
          .go();
    } finally {
      test("ALTER SESSION SET `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }

  @Test
  public void join() throws Exception {
    try {
      test("ALTER SESSION SET `planner.slice_target` = 1");
      test("CREATE TABLE dfs_test.tmp.lineitem AS SELECT * FROM cp.`tpch/lineitem.parquet`");
      test("CREATE TABLE dfs_test.tmp.orders AS select * FROM cp.`tpch/orders.parquet`");
      test("ANALYZE TABLE dfs_test.tmp.lineitem COMPUTE STATISTICS FOR ALL COLUMNS");
      test("ANALYZE TABLE dfs_test.tmp.orders COMPUTE STATISTICS FOR ALL COLUMNS");
      test("SELECT * FROM dfs_test.tmp.`lineitem/.stats.drill`");
      test("SELECT * FROM dfs_test.tmp.`orders/.stats.drill`");

      test("SELECT * FROM dfs_test.tmp.`lineitem` l JOIN dfs_test.tmp.`orders` o ON l.l_orderkey = o.o_orderkey");
    } finally {
      test("ALTER SESSION SET `planner.slice_target` = " + ExecConstants.SLICE_TARGET_DEFAULT);
    }
  }
}
