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
package org.apache.drill.exec.physical.impl.unpivot;

import java.util.List;
import java.util.Map;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.UnpivotMaps;
import org.apache.drill.exec.record.AbstractSingleRecordBatch;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.drill.exec.vector.complex.MapVector;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class UnpivotMapsRecordBatch extends AbstractSingleRecordBatch<UnpivotMaps> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UnpivotMapsRecordBatch.class);

  private final String keyField;
  private final List<String> dataFields;

  WritableBatch incomingData;
  VarCharVector keyVec;
  int keyIndex = 0;
  List<String> keyList = null;
  Map<MaterializedField, Map<String, ValueVector>> dataSrcVecMap = null;
  Map<MaterializedField, ValueVector> copySrcVecMap = null;

  List<TransferPair> transferList;

  private boolean hasRemainder = false;
  private int remainderIndex = 0;
  private int recordCount = 0;

  public UnpivotMapsRecordBatch(UnpivotMaps pop, RecordBatch incoming, FragmentContext context)
      throws OutOfMemoryException {
    super(pop, context, incoming);
    this.keyField = pop.getKeyField();
    this.dataFields = pop.getDataFields();

    //TODO: fix this... it's terrible...
    dataFields.add(keyField);
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  @Override
  public IterOutcome innerNext() {
    if (hasRemainder) {
      handleRemainder();
      return IterOutcome.OK;

    } else if (keyIndex != 0) {
      doWork();
      return IterOutcome.OK;
    } else {
      return super.innerNext();

    }
  }

  public VectorContainer getOutgoingContainer() {
    return this.container;
  }

  private void handleRemainder() {
    throw new UnsupportedOperationException("Remainder batch not supported!");

    // TODO: handle remainder batch!!!
  }

  private int doTransfer() {
    int n;

    n = incoming.getRecordCount();

    for (TransferPair tp : transferList) {
      tp.splitAndTransfer(0, n);
    }

    return n;
  }

  @Override
  protected IterOutcome doWork() {
    int outRecordCount = incoming.getRecordCount();

    prepareTransfers();

    int inRecordCount = doTransfer();

    if (inRecordCount < outRecordCount) {
      hasRemainder = true;
      remainderIndex = outRecordCount;
      this.recordCount = remainderIndex;
      return IterOutcome.OK;
    }


    keyIndex = (keyIndex + 1) % keyList.size();
    this.recordCount = outRecordCount;

    if (keyIndex == 0) {
      for (VectorWrapper w : incoming) {
        w.clear();
      }
    }
    return IterOutcome.OK;
  }

  private void buildKeyList() {
    for (VectorWrapper<?> vw : incoming) {
      String kf = vw.getField().getLastName();

      if (kf != keyField) {
        continue;
      }

      // assert keyList == null;
      keyList = Lists.newArrayList();

      for (ValueVector vv : (MapVector) vw.getValueVector()) {
        keyList.add(vv.getField().getLastName());
      }
    }
  }

  private void dostuff() {
    dataSrcVecMap = Maps.newHashMap();
    copySrcVecMap = Maps.newHashMap();
    for (VectorWrapper<?> vw : incoming) {
      MaterializedField ds = vw.getField();
      String col = vw.getField().getLastName();

      if (!dataFields.contains(col)) {
        MajorType mt = vw.getValueVector().getField().getType();
        MaterializedField mf = MaterializedField.create(col, mt);
        container.add(TypeHelper.getNewVector(mf, oContext.getAllocator()));
        copySrcVecMap.put(mf, vw.getValueVector());
        continue;
      }

      MapVector mv = (MapVector) vw.getValueVector();
      assert mv.getPrimitiveVectors().size() > 0;

      MajorType mt = mv.iterator().next().getField().getType();
      MaterializedField mf = MaterializedField.create(col, mt);
      assert !dataSrcVecMap.containsKey(mf);
      container.add(TypeHelper.getNewVector(mf, oContext.getAllocator()));

      Map<String, ValueVector> m = Maps.newHashMap();
      dataSrcVecMap.put(mf, m);

      for (ValueVector vv : mv) {
        String k = vv.getField().getLastName();

        if (!keyList.contains(k)) {
          throw new UnsupportedOperationException("Unpivot data vector " +
              ds + " contains key " + k + " not contained in key source!");
        }

        if (vv.getField().getType().getMinorType() == MinorType.MAP) {
          throw new UnsupportedOperationException("Unpivot of nested map is " +
              "not supported!");
        }

        m.put(vv.getField().getLastName(), vv);
      }
    }

    container.buildSchema(incoming.getSchema().getSelectionVectorMode());
  }

  private void prepareTransfers() {
    transferList = Lists.newArrayList();
    for (VectorWrapper<?> vw : container) {
      MaterializedField mf = vw.getField();

      ValueVector vv;
      TransferPair tp;
      if (dataSrcVecMap.containsKey(mf)) {
        String k = keyList.get(keyIndex);
        vv = dataSrcVecMap.get(mf).get(k);
        tp = vv.makeTransferPair(vw.getValueVector());
      } else {
        vv = copySrcVecMap.get(mf);
        tp = vv.makeTransferPair(vw.getValueVector());
      }

      transferList.add(tp);
    }
  }

  @Override
  protected boolean setupNewSchema()
      throws SchemaChangeException {
    if (incoming.getSchema().getSelectionVectorMode() != SelectionVectorMode.NONE) {
      throw new UnsupportedOperationException("Selection vector not supported by unpivotMaps");
    }

    container.clear();

    buildKeyList();
    dostuff();
    return true;
  }
}
