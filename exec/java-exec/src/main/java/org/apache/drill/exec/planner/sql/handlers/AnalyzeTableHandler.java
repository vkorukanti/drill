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
package org.apache.drill.exec.planner.sql.handlers;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.PlannerType;
import org.apache.drill.exec.planner.logical.DrillAnalyzeRel;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.logical.DrillWriterRel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.apache.drill.exec.planner.sql.DrillSqlWorker;
import org.apache.drill.exec.planner.sql.SchemaUtilites;
import org.apache.drill.exec.planner.sql.parser.SqlAnalyzeTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;

import java.io.IOException;

public class AnalyzeTableHandler extends DefaultSqlHandler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AnalyzeTableHandler.class);

  public AnalyzeTableHandler(SqlHandlerConfig config) {
    super(config);
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode)
      throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    final SqlAnalyzeTable sqlAnalyzeTable = unwrap(sqlNode, SqlAnalyzeTable.class);

    SqlIdentifier tableIdentifier = sqlAnalyzeTable.getTableIdentifier();
    SqlNodeList allNodes = new SqlNodeList(SqlParserPos.ZERO);
    allNodes.add(new SqlIdentifier("*", SqlParserPos.ZERO));
    SqlSelect scanSql = new SqlSelect(
        SqlParserPos.ZERO, /* position */
        SqlNodeList.EMPTY, /* keyword list */
        allNodes, /*select list */
        tableIdentifier, /* from */
        null, /* where */
        null, /* group by */
        null, /* having */
        null, /* windowDecls */
        null, /* orderBy */
        null, /* offset */
        null /* fetch */
    );

    final ConvertedRelNode convertedRelNode = validateAndConvert(rewrite(scanSql));
    final RelDataType validatedRowType = convertedRelNode.getValidatedRowType();
    final RelNode relScan = convertedRelNode.getConvertedNode();

    // don't analyze all columns
//      List<String> analyzeFields = sqlAnalyzeTable.getFieldNames();
//      if (analyzeFields.size() > 0) {
//        RelDataType rowType = new DrillFixedRelDataTypeImpl(
//            planner.getTypeFactory(), analyzeFields);
//        relScan = RelOptUtil.createCastRel(relScan, rowType, true);
//      }

    final AbstractSchema drillSchema = SchemaUtilites.resolveToMutableDrillSchema(
        config.getConverter().getDefaultSchema(), sqlAnalyzeTable.getSchemaPath());

    // Convert the query to Drill Logical plan and insert a writer operator on top.
    DrillRel drel = convertToDrel(relScan, drillSchema, sqlAnalyzeTable.getName());
    Prel prel = convertToPrel(drel);
    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    log("Drill Plan", plan, logger);

    return plan;
  }

  protected DrillRel convertToDrel(RelNode relNode, AbstractSchema schema, String analyzeTableName)
      throws RelConversionException, SqlUnsupportedException {
    final DrillRel convertedRelNode = convertToDrel(relNode);
    if (convertedRelNode instanceof DrillStoreRel) {
      throw new UnsupportedOperationException();
    } else {
      RelNode writerRel = new DrillWriterRel(
          convertedRelNode.getCluster(),
          convertedRelNode.getTraitSet(),
          new DrillAnalyzeRel(
              convertedRelNode.getCluster(),
              convertedRelNode.getTraitSet(),
              convertedRelNode
          ),
          schema.appendToMetadataTable(analyzeTableName)
      );
      return new DrillScreenRel(writerRel.getCluster(), writerRel.getTraitSet(), writerRel);
    }
  }
}
