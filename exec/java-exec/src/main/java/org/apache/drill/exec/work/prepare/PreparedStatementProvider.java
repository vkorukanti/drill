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
package org.apache.drill.exec.work.prepare;

import org.apache.drill.common.exceptions.ErrorHelper;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType;
import org.apache.drill.exec.proto.UserBitShared.QueryResult;
import org.apache.drill.exec.proto.UserBitShared.QueryResult.QueryState;
import org.apache.drill.exec.proto.UserBitShared.QueryType;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.proto.UserProtos.ColumnSearchability;
import org.apache.drill.exec.proto.UserProtos.ColumnUpdatability;
import org.apache.drill.exec.proto.UserProtos.CreatePreparedStatementReq;
import org.apache.drill.exec.proto.UserProtos.CreatePreparedStatementResp;
import org.apache.drill.exec.proto.UserProtos.PreparedStatement;
import org.apache.drill.exec.proto.UserProtos.PreparedStatementHandle;
import org.apache.drill.exec.proto.UserProtos.RequestStatus;
import org.apache.drill.exec.proto.UserProtos.ResultColumnMetadata;
import org.apache.drill.exec.proto.UserProtos.RunQuery;
import org.apache.drill.exec.proto.UserProtos.UserToBitHandshake;
import org.apache.drill.exec.rpc.Acks;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnection;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnectionImpl;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.ischema.InfoSchemaConstants;
import org.apache.drill.exec.work.user.UserWorker;

import com.google.common.collect.ImmutableMap;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class PreparedStatementProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PreparedStatementProvider.class);

  private static final Map<MinorType, String> TYPE_MAPPING = ImmutableMap.<MinorType, String>builder()
      .put(TypeProtos.MinorType.INT, "INT")
      .put(TypeProtos.MinorType.BIGINT, "BIGINT")
      .put(TypeProtos.MinorType.FLOAT4, "FLOAT")
      .put(TypeProtos.MinorType.FLOAT8, "DOUBLE")
      .put(TypeProtos.MinorType.VARCHAR, "CHARACTER VARYING")
      .put(TypeProtos.MinorType.BIT, "BOOLEAN")
      .put(TypeProtos.MinorType.DATE, "DATE")
      .put(TypeProtos.MinorType.DECIMAL9, "DECIMAL")
      .put(TypeProtos.MinorType.DECIMAL18, "DECIMAL")
      .put(TypeProtos.MinorType.DECIMAL28SPARSE, "DECIMAL")
      .put(TypeProtos.MinorType.DECIMAL38SPARSE, "DECIMAL")
      .put(TypeProtos.MinorType.TIME, "TIME")
      .put(TypeProtos.MinorType.TIMESTAMP, "TIMESTAMP")
      .put(TypeProtos.MinorType.VARBINARY, "BINARY VARYING")
      .put(TypeProtos.MinorType.INTERVALYEAR, "INTERVAL")
      .put(TypeProtos.MinorType.INTERVALDAY, "INTERVAL")
      .put(TypeProtos.MinorType.MAP, "MAP")
      .put(TypeProtos.MinorType.LIST, "LIST")
      .build();

  private final UserWorker userWorker;

  public PreparedStatementProvider(final UserWorker userWorker) {
    this.userWorker = userWorker;
  }

  public CreatePreparedStatementResp createPreparedStatement(UserClientConnectionImpl connection,
      CreatePreparedStatementReq req) {

    final CreatePreparedStatementResp.Builder respBuilder = CreatePreparedStatementResp.newBuilder();

    UserClientConnectionWrapper wrapper = new UserClientConnectionWrapper(connection);

    final RunQuery limit0Query =
        RunQuery.newBuilder()
            .setType(QueryType.SQL)
            .setPlan(String.format("SELECT * FROM (%s) LIMIT 0", req.getSqlQuery()))
            .build();

    userWorker.submitWork(wrapper, limit0Query);

    try {
      wrapper.await();
    } catch (InterruptedException e) {
      respBuilder.setStatus(RequestStatus.FAILED);
      final String errorId = UUID.randomUUID().toString();
      logger.error("Prepared statement creation interrupted. ErrorId: {}", errorId, e);
      respBuilder.setError(createDrillPBError(e, errorId));
      return respBuilder.build();
    }

    if (wrapper.getError() != null) {
      respBuilder.setStatus(RequestStatus.FAILED);
      logger.error("Failed to get result set schema for prepare statement");
      respBuilder.setError(wrapper.getError());
      return respBuilder.build();
    }

    PreparedStatement.Builder prepStmtBuilder = PreparedStatement.newBuilder();

    for(SerializedField field : wrapper.getFields()) {
      prepStmtBuilder.addColumns(serializeColumn(field));
    }

    prepStmtBuilder.setPreparedStatementHandle(
        PreparedStatementHandle
            .newBuilder()
            .setSqlQuery(req.getSqlQuery())
            .build()
            .toByteString()
    );

    respBuilder.setPreparedStatement(prepStmtBuilder.build());

    return respBuilder.build();
  }

  private static class UserClientConnectionWrapper implements UserClientConnection {
    private final UserClientConnection inner;
    private final CountDownLatch latch = new CountDownLatch(1);

    private DrillPBError error;
    private List<SerializedField> fields;

    UserClientConnectionWrapper(UserClientConnection inner) {
      this.inner = inner;
    }

    @Override
    public void disableReadTimeout() {
      inner.disableReadTimeout();
    }

    @Override
    public void setUser(UserToBitHandshake inbound)
        throws IOException {
      inner.setUser(inbound);
    }

    @Override
    public UserSession getSession() {
      return inner.getSession();
    }

    @Override
    public void sendResult(RpcOutcomeListener<Ack> listener, QueryResult result) {
      QueryState state = result.getQueryState();
      if (state == QueryState.FAILED || state  == QueryState.CANCELED || state == QueryState.COMPLETED) {
        if (state == QueryState.FAILED) {
          error = result.getError(0);
        }
        latch.countDown();
      }

      listener.success(Acks.OK, null);
    }

    @Override
    public void sendData(RpcOutcomeListener<Ack> listener, QueryWritableBatch result) {
      if (fields == null) {
        fields = result.getHeader().getDef().getFieldList();
      }

      for(ByteBuf buf : result.getBuffers()) {
        buf.release();
      }

      listener.success(Acks.OK, null);
    }

    @Override
    public Channel getConnectionChannel() {
      return inner.getConnectionChannel();
    }

    void await() throws InterruptedException {
      latch.await();
    }

    DrillPBError getError() {
      return error;
    }

    List<SerializedField> getFields() {
      return fields;
    }
  }

  private DrillPBError createDrillPBError(final Exception ex, final String errorId) {
    final DrillPBError.Builder builder = DrillPBError.newBuilder();
    builder.setErrorType(ErrorType.SYSTEM);
    builder.setErrorId(errorId);
    if (ex.getMessage() != null) {
      builder.setMessage(ex.getMessage());
    }

    builder.setException(ErrorHelper.getWrapper(ex));

    return builder.build();
  }


  private ResultColumnMetadata serializeColumn(SerializedField field) {
    ResultColumnMetadata.Builder builder = ResultColumnMetadata.newBuilder();

    /**
     * Defaults to "DRILL" as drill as only one catalog.
     */
    builder.setCatalogName(InfoSchemaConstants.IS_CATALOG_NAME);

    /**
     * Designated column's schema name. Empty string if not applicable. Initial implementation defaults to value as
     * we use LIMIT 0 queries to get the schema and schem info is lost. If we derive the schema from plan, we may get
     * the right value.
     */
    //builder.setSchemaName()

    /**
     * Designated column's table name. Not set if not applicable. Initial implementation defaults to no value as
     * we use LIMIT 0 queries to get the schema and schem info is lost. If we derive the schema from plan, we may get
     * the right value.
     */
    // builder.setTableName()

    builder.setColumnName(field.getNamePart().getName());

    /**
     * Data type in string format. Value is SQL standard type.
     */
    builder.setDataType(getDataTypeFromMajorType(field.getMajorType()));

    builder.setIsNullable(field.getMajorType().getMode() == DataMode.OPTIONAL);

    /**
     * For numeric data, this is the maximum precision.
     * For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation
     *    (assuming the maximum allowed precision of the fractional seconds component).
     * For binary data, this is the length in bytes.
     * For all other types 0 is returned where the column size is not applicable.
     */
    builder.setPrecision(getPrecisionFromMajorType(field.getMajorType()));


    /**
     * Column's number of digits to right of the decimal point. 0 is returned for types where the scale is not applicable
     */
    builder.setScale(getScaleFromMajorType(field.getMajorType()));

    /**
     * Indicates whether values in the designated column are signed numbers.
     */
    builder.setSigned(getSignedFromMajorType(field.getMajorType()));

    /**
     * Is the column an aliased column. Initial implementation defaults to true as we derive schema from LIMIT 0 query and
     * not plan
     */
    builder.setIsAliased(true);

    builder.setSearchability(ColumnSearchability.ALL);
    builder.setUpdatability(ColumnUpdatability.READ_ONLY);
    builder.setAutoIncrement(false);
    builder.setCaseSensitivity(true);
    builder.setSortable(isSortableFromMajorType(field.getMajorType()));

    /**
     * TODO:
     * Returns the fully-qualified name of the Java class whose instances are manufactured if the method
     * ResultSet.getObject is called to retrieve a value from the column. Applicable only to JDBC clients.
     */
    //builder.setClassName()

    return builder.build();
  }

  private String getDataTypeFromMajorType(MajorType type) {
    return TYPE_MAPPING.get(type.getMinorType());
  }

  private int getPrecisionFromMajorType(MajorType majorType) {
    MinorType type = majorType.getMinorType();

    if (type == MinorType.DECIMAL9 || type == MinorType.DECIMAL18 || type == MinorType.DECIMAL28SPARSE ||
        type == MinorType.DECIMAL38SPARSE) {
      return majorType.getPrecision();
    }

    if (type == MinorType.VARBINARY || type == MinorType.VARCHAR) {
      return Short.MAX_VALUE;
    }

    // TODO: handle datetime type
    return 0;
  }

  private int getScaleFromMajorType(MajorType majorType) {
    MinorType type = majorType.getMinorType();

    if (type == MinorType.DECIMAL9 || type == MinorType.DECIMAL18 || type == MinorType.DECIMAL28SPARSE ||
        type == MinorType.DECIMAL38SPARSE) {
      return majorType.getScale();
    }

    return 0;
  }

  private boolean getSignedFromMajorType(MajorType majorType) {
    MinorType type = majorType.getMinorType();

    return type == MinorType.INT ||
        type == MinorType.BIGINT ||
        type == MinorType.FLOAT4 ||
        type == MinorType.FLOAT8;
  }

  private boolean isSortableFromMajorType(MajorType majorType) {
    return majorType.getMinorType() != MinorType.MAP && majorType.getMinorType() == MinorType.LIST;
  }
}
