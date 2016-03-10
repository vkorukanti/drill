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
package org.apache.drill.exec.physical.impl.aggregate;

import com.google.common.collect.Lists;
import com.sun.codemodel.JExpr;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.FunctionCallFactory;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.StatisticsAggregate;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.FieldIdUtil;
import org.apache.drill.exec.vector.complex.MapVector;

import java.io.IOException;
import java.util.List;

public class StatisticsAggBatch extends StreamingAggBatch {
  private List<String> functions;
  private int schema = 0;

  public StatisticsAggBatch(StatisticsAggregate popConfig, RecordBatch incoming, FragmentContext context)
      throws OutOfMemoryException {
    super(popConfig, incoming, context);
    this.functions = popConfig.getFunctions();
  }

  private void createKeyColumn(String name, LogicalExpression expr, List<LogicalExpression> keyExprs, List<TypedFieldId> keyOutputIds)
      throws SchemaChangeException {
    ErrorCollector collector = new ErrorCollectorImpl();

    final LogicalExpression mle = ExpressionTreeMaterializer.materialize(
        expr,
        incoming,
        collector,
        context.getFunctionRegistry()
    );

    final MaterializedField outputField = MaterializedField.create(
        name,
        mle.getMajorType()
    );
    ValueVector vector = TypeHelper.getNewVector(
        outputField,
        oContext.getAllocator()
    );

    keyExprs.add(mle);
    keyOutputIds.add(container.add(vector));

    if (collector.hasErrors()) {
      throw new SchemaChangeException(
          "Failure while materializing expression. " + collector.toErrorString());
    }
  }

  private void createNestedKeyColumn(MapVector parent, String name, LogicalExpression expr, List<LogicalExpression> keyExprs, List<TypedFieldId> keyOutputIds)
      throws SchemaChangeException {
    ErrorCollector collector = new ErrorCollectorImpl();

    LogicalExpression mle = ExpressionTreeMaterializer.materialize(
        expr,
        incoming,
        collector,
        context.getFunctionRegistry()
    );

    Class<? extends ValueVector> vvc =
        TypeHelper.getValueVectorClass(mle.getMajorType().getMinorType(), mle.getMajorType().getMode());

    ValueVector vv = parent.addOrGet(name, mle.getMajorType(), vvc);

    TypedFieldId pfid = container.getValueVectorId(SchemaPath.getSimplePath(parent.getField().getPath()));
    assert pfid.getFieldIds().length == 1;
    TypedFieldId.Builder builder = TypedFieldId.newBuilder();
    builder.addId(pfid.getFieldIds()[0]);
    TypedFieldId id = FieldIdUtil.getFieldIdIfMatches(parent, builder, true,
        SchemaPath.getSimplePath(vv.getField().getPath()).getRootSegment()
    );

    keyExprs.add(mle);
    keyOutputIds.add(id);

    if (collector.hasErrors()) {
      throw new SchemaChangeException(
          "Failure while materializing expression. " + collector.toErrorString());
    }
  }

  private void addMapVector(String name, MapVector parent, LogicalExpression expr, List<LogicalExpression> valueExprs)
      throws SchemaChangeException {
    ErrorCollector collector = new ErrorCollectorImpl();

    LogicalExpression mle = ExpressionTreeMaterializer.materialize(
        expr,
        incoming,
        collector,
        context.getFunctionRegistry()
    );

    Class<? extends ValueVector> vvc =
        (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(
            mle.getMajorType().getMinorType(),
            mle.getMajorType().getMode()
        );
    ValueVector vv = parent.addOrGet(name, mle.getMajorType(), vvc);

    TypedFieldId pfid = container.getValueVectorId(SchemaPath.getSimplePath(parent.getField().getPath()));
    assert pfid.getFieldIds().length == 1;
    TypedFieldId.Builder builder = TypedFieldId.newBuilder();
    builder.addId(pfid.getFieldIds()[0]);
    TypedFieldId id = FieldIdUtil.getFieldIdIfMatches(parent, builder, true,
        SchemaPath.getSimplePath(vv.getField().getPath()).getRootSegment()
    );

    valueExprs.add(new ValueVectorWriteExpression(id, mle, true));

    if (collector.hasErrors()) {
      throw new SchemaChangeException(
          "Failure while materializing expression. " + collector.toErrorString());
    }
  }

  private StreamingAggregator codegenAggregator(List<LogicalExpression> keyExprs, List<LogicalExpression> valueExprs, List<TypedFieldId> keyOutputIds)
      throws SchemaChangeException, ClassTransformationException, IOException {
    ClassGenerator<StreamingAggregator> cg = CodeGenerator.getRoot(StreamingAggTemplate.TEMPLATE_DEFINITION, context.getFunctionRegistry());

    LogicalExpression[] keyExprsArray = new LogicalExpression[keyExprs.size()];
    LogicalExpression[] valueExprsArray = new LogicalExpression[valueExprs.size()];
    TypedFieldId[] keyOutputIdsArray = new TypedFieldId[keyOutputIds.size()];

    keyExprs.toArray(keyExprsArray);
    valueExprs.toArray(valueExprsArray);
    keyOutputIds.toArray(keyOutputIdsArray);

    setupIsSame(cg, keyExprsArray);
    setupIsSameApart(cg, keyExprsArray);
    addRecordValues(cg, valueExprsArray);
    outputRecordKeys(cg, keyOutputIdsArray, keyExprsArray);
    outputRecordKeysPrev(cg, keyOutputIdsArray, keyExprsArray);

    cg.getBlock("resetValues")._return(JExpr.TRUE);
    getIndex(cg);

    container.buildSchema(SelectionVectorMode.NONE);
    StreamingAggregator agg = context.getImplementationClass(cg);
    agg.setup(oContext, incoming, this);
    return agg;
  }

  protected StreamingAggregator createAggregatorInternal()
      throws SchemaChangeException, ClassTransformationException, IOException {
    container.clear();

    List<LogicalExpression> keyExprs = Lists.newArrayList();
    List<LogicalExpression> valueExprs = Lists.newArrayList();
    List<TypedFieldId> keyOutputIds = Lists.newArrayList();

    createKeyColumn("schema",
        ValueExpressions.getBigInt(schema++),
        keyExprs,
        keyOutputIds
    );
    createKeyColumn("computed",
        ValueExpressions.getBigInt(System.currentTimeMillis()),
        keyExprs,
        keyOutputIds
    );

    MapVector cparent = new MapVector("column", oContext.getAllocator(), null);
    container.add(cparent);
    for (MaterializedField mf : incoming.getSchema()) {
      createNestedKeyColumn(
          cparent,
          mf.getLastName(),
          ValueExpressions.getChar(mf.getLastName()),
          keyExprs,
          keyOutputIds
      );
    }

    for (String func : functions) {
      MapVector parent = new MapVector(func, oContext.getAllocator(), null);
      container.add(parent);

      for (MaterializedField mf : incoming.getSchema()) {
        List<LogicalExpression> args = Lists.newArrayList();
        args.add(SchemaPath.getSimplePath(mf.getPath()));
        LogicalExpression call = FunctionCallFactory.createExpression(func, args);

        addMapVector(mf.getLastName(), parent, call, valueExprs);
      }
    }

    return codegenAggregator(keyExprs, valueExprs, keyOutputIds);
  }
}
