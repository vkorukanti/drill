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
package org.apache.drill.exec.planner.common;

import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.rest.QueryWrapper.Listener;

public class DrillTableMetadata {
  private final Table statsTable;
  private final String statsTableString;
  private final Map<String, Long> ndv = Maps.newHashMap();

  private double rowcount = -1;
  private boolean materialized = false;

  public DrillTableMetadata(Table statsTable, String statsTableString) {
    // TODO: this should actually be a Table object or a selection or something instead of a string
    this.statsTable = statsTable;
    this.statsTableString = statsTableString;
  }

  public Double getNdv(String col) {
    final String upperCol = col.toUpperCase();
    final Long ndvCol = ndv.get(upperCol);
    if (ndvCol != null) {
      return Math.min(ndvCol, rowcount);
    }

    return null;
  }

  public Double getRowCount() {
    return rowcount > 0 ? new Double(rowcount) : null;
  }

  public void materialize(QueryContext context) throws Exception {
    if (materialized) {
      return;
    }

    if (statsTableString == null || statsTable == null) {
      return;
    }

    materialized = true;

    final String sql = "select a.* from " + statsTableString +
        " as a inner join " +
        "(select `column`, max(`computed`) as `computed` " +
        "from " + statsTableString + " group by `column`) as b on a.`column` = b.`column` and a.`computed` = b.`computed`";

    final DrillbitContext dc = context.getDrillbitContext();
    try(final DrillClient client = new DrillClient(dc.getConfig(), dc.getClusterCoordinator(), dc.getAllocator())) {
      final Listener listener = new Listener(dc.getAllocator());

      client.connect();
      client.runQuery(UserBitShared.QueryType.SQL, sql, listener);

      listener.waitForCompletion();

      for (Map<String, String> r : listener.results) {
        ndv.put(r.get("column").toUpperCase(), Long.valueOf(r.get("ndv")));
        rowcount = Math.max(rowcount, Long.valueOf(r.get("statcount")));
      }
    }
  }

  /**
   * materialize on nodes that have an attached metadata object
   */
  public static class MaterializationVisitor extends RelVisitor {
    private QueryContext context;

    public static void materialize(final RelNode relNode, final QueryContext context) {
      new MaterializationVisitor(context).go(relNode);
    }

    private MaterializationVisitor(QueryContext context) {
      this.context = context;
    }

    @Override
    public void visit(
        RelNode node,
        int ordinal,
        RelNode parent) {
      if (node instanceof TableScan) {
        try {
          DrillTable dt = node.getTable().unwrap(DrillTable.class);
          if (dt.getDrillTableMetadata() != null) {
            dt.getDrillTableMetadata().materialize(context);
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      super.visit(node, ordinal, parent);
    }

  }
}
