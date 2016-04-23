/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;

import java.util.LinkedList;
import java.util.List;

public class DrillTimestampDiffConvertlet implements SqlRexConvertlet {

  public final static DrillTimestampDiffConvertlet INSTANCE = new DrillTimestampDiffConvertlet();

  private DrillTimestampDiffConvertlet() {
  }

  @Override
  public RexNode convertCall(SqlRexContext cx, SqlCall call) {
    final List<SqlNode> operands = call.getOperandList();
    final List<RexNode> exprs = new LinkedList<>();

    for (SqlNode node: operands) {
      exprs.add(cx.convertExpression(node));
    }

    final RelDataTypeFactory typeFactory = cx.getTypeFactory();
    final RelDataType returnType =
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            exprs.get(1).getType().isNullable() || exprs.get(2).getType().isNullable());

    return cx.getRexBuilder().makeCall(returnType, call.getOperator(), exprs);
  }
}