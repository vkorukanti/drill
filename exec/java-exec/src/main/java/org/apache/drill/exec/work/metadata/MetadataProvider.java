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
package org.apache.drill.exec.work.metadata;

import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.CATS_COL_CATALOG_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.SCHS_COL_SCHEMA_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.SHRD_COL_TABLE_NAME;
import static org.apache.drill.exec.store.ischema.InfoSchemaConstants.SHRD_COL_TABLE_SCHEMA;

import org.apache.drill.common.exceptions.ErrorHelper;
import org.apache.drill.exec.ops.ViewExpansionContext;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError.ErrorType;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.proto.UserProtos.GetCatalogsResp;
import org.apache.drill.exec.proto.UserProtos.GetCatalogsReq;
import org.apache.drill.exec.proto.UserProtos.GetColumnsReq;
import org.apache.drill.exec.proto.UserProtos.GetColumnsResp;
import org.apache.drill.exec.proto.UserProtos.GetSchemasReq;
import org.apache.drill.exec.proto.UserProtos.GetSchemasResp;
import org.apache.drill.exec.proto.UserProtos.GetTablesReq;
import org.apache.drill.exec.proto.UserProtos.GetTablesResp;
import org.apache.drill.exec.proto.UserProtos.LikeFilter;
import org.apache.drill.exec.proto.UserProtos.RequestStatus;
import org.apache.drill.exec.rpc.user.UserServer.UserClientConnectionImpl;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.server.options.OptionValue;
import org.apache.drill.exec.store.SchemaConfig.SchemaConfigInfoProvider;
import org.apache.drill.exec.store.SchemaTreeProvider;
import org.apache.drill.exec.store.ischema.InfoSchemaConstants;
import org.apache.drill.exec.store.ischema.InfoSchemaFilter;
import org.apache.drill.exec.store.ischema.InfoSchemaFilter.ConstantExprNode;
import org.apache.drill.exec.store.ischema.InfoSchemaFilter.ExprNode;
import org.apache.drill.exec.store.ischema.InfoSchemaFilter.FieldExprNode;
import org.apache.drill.exec.store.ischema.InfoSchemaFilter.FunctionExprNode;
import org.apache.drill.exec.store.ischema.InfoSchemaTableType;
import org.apache.drill.exec.store.ischema.Records.Catalog;
import org.apache.drill.exec.store.ischema.Records.Column;
import org.apache.drill.exec.store.ischema.Records.Schema;
import org.apache.drill.exec.store.ischema.Records.Table;
import org.apache.drill.exec.store.pojo.PojoRecordReader;
import org.apache.drill.exec.work.WorkManager.WorkerBee;

import com.google.common.collect.ImmutableList;

import java.util.UUID;

public class MetadataProvider {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MetadataProvider.class);

  private static final String LIKE_FUNCTION = "like";
  private static final String AND_FUNCTION = "booleanand";
  private static final String OR_FUNCTION = "booleanor";

  private final WorkerBee bee;

  private final SchemaConfigInfoProvider provider = new SchemaConfigInfoProvider() {
    @Override
    public ViewExpansionContext getViewExpansionContext() {
      throw new UnsupportedOperationException("View expansion context is not supported");
    }

    @Override
    public OptionValue getOption(String optionKey) {
      throw new UnsupportedOperationException("Not supported");
    }
  };

  public MetadataProvider(final WorkerBee bee) {
    this.bee = bee;
  }

  public GetCatalogsResp getCatalogs(UserClientConnectionImpl connection, GetCatalogsReq req) {

    final GetCatalogsResp.Builder respBuilder = GetCatalogsResp.newBuilder();

    final InfoSchemaFilter filter = createInfoSchemaFilter(
        req.hasCatalogNameFilter() ? req.getCatalogNameFilter() : null, null, null, null);

    final UserSession userSession = connection.getSession();
    try (SchemaTreeProvider schemaTreeProvider = new SchemaTreeProvider(bee.getContext())) {
      final InfoSchemaTableType tableType = InfoSchemaTableType.CATALOGS;

      final PojoRecordReader<Catalog> records = (PojoRecordReader<Catalog>)
          tableType.getRecordReader(
              schemaTreeProvider.getRootSchema(userSession.getCredentials().getUserName(), provider),
              filter, userSession.getOptions());

      for(Catalog c : records) {
        final UserProtos.CatalogMetadata.Builder catBuilder = UserProtos.CatalogMetadata.newBuilder();
        catBuilder.setCatalogName(c.CATALOG_NAME);
        catBuilder.setDescription(c.CATALOG_DESCRIPTION);
        catBuilder.setConnect(c.CATALOG_CONNECT);

        respBuilder.addCatalogs(catBuilder.build());
      }

      respBuilder.setStatus(RequestStatus.OK);
    } catch (Exception e) {
      respBuilder.setStatus(RequestStatus.FAILED);
      respBuilder.setError(createPBError("get catalogs", e));
    }

    return respBuilder.build();
  }

  public GetSchemasResp getSchemas(UserClientConnectionImpl connection, GetSchemasReq req) {
    final GetSchemasResp.Builder respBuilder = GetSchemasResp.newBuilder();

    final InfoSchemaFilter filter = createInfoSchemaFilter(
        req.hasCatalogNameFilter() ? req.getCatalogNameFilter() : null,
        req.hasSchameNameFilter() ? req.getSchameNameFilter() : null,
        null, null);

    final UserSession userSession = connection.getSession();
    try (SchemaTreeProvider schemaTreeProvider = new SchemaTreeProvider(bee.getContext())) {
      final InfoSchemaTableType tableType = InfoSchemaTableType.SCHEMATA;

      final PojoRecordReader<Schema> records = (PojoRecordReader<Schema>)
          tableType.getRecordReader(
              schemaTreeProvider.getRootSchema(userSession.getCredentials().getUserName(), provider),
              filter, userSession.getOptions());

      for(Schema s : records) {
        final UserProtos.SchemaMetadata.Builder schemaBuilder = UserProtos.SchemaMetadata.newBuilder();
        schemaBuilder.setCatalogName(s.CATALOG_NAME);
        schemaBuilder.setSchemaName(s.SCHEMA_NAME);
        schemaBuilder.setOwner(s.SCHEMA_OWNER);
        schemaBuilder.setType(s.TYPE);
        schemaBuilder.setMutable(s.IS_MUTABLE);

        respBuilder.addSchemas(schemaBuilder.build());
      }

      respBuilder.setStatus(RequestStatus.OK);
    } catch (Exception e) {
      respBuilder.setStatus(RequestStatus.FAILED);
      respBuilder.setError(createPBError("get schemas", e));
    }

    return respBuilder.build();
  }

  public GetTablesResp getTables(UserClientConnectionImpl connection, GetTablesReq req) {
    final GetTablesResp.Builder respBuilder = GetTablesResp.newBuilder();

    final InfoSchemaFilter filter = createInfoSchemaFilter(
        req.hasCatalogNameFilter() ? req.getCatalogNameFilter() : null,
        req.hasSchameNameFilter() ? req.getSchameNameFilter() : null,
        req.hasTableNameFilter() ? req.getTableNameFilter() : null,
        null);

    final UserSession userSession = connection.getSession();
    try (SchemaTreeProvider schemaTreeProvider = new SchemaTreeProvider(bee.getContext())) {
      final InfoSchemaTableType tableType = InfoSchemaTableType.TABLES;

      final PojoRecordReader<Table> records = (PojoRecordReader<Table>)
          tableType.getRecordReader(
              schemaTreeProvider.getRootSchema(userSession.getCredentials().getUserName(), provider),
              filter, userSession.getOptions());

      for(Table t : records) {
        final UserProtos.TableMetadata.Builder tableBuilder = UserProtos.TableMetadata.newBuilder();
        tableBuilder.setCatalogName(t.TABLE_CATALOG);
        tableBuilder.setSchemaName(t.TABLE_SCHEMA);
        tableBuilder.setTableName(t.TABLE_NAME);
        tableBuilder.setType(t.TABLE_TYPE);

        respBuilder.addTables(tableBuilder.build());
      }

      respBuilder.setStatus(RequestStatus.OK);
    } catch (Exception e) {
      respBuilder.setStatus(RequestStatus.FAILED);
      respBuilder.setError(createPBError("get tables", e));
    }

    return respBuilder.build();
  }

  public GetColumnsResp getColumns(UserClientConnectionImpl connection, GetColumnsReq req) {
    final GetColumnsResp.Builder respBuilder = GetColumnsResp.newBuilder();

    final InfoSchemaFilter filter = createInfoSchemaFilter(
        req.hasCatalogNameFilter() ? req.getCatalogNameFilter() : null,
        req.hasSchameNameFilter() ? req.getSchameNameFilter() : null,
        req.hasTableNameFilter() ? req.getTableNameFilter() : null,
        req.hasColumnNameFilter() ? req.getColumnNameFilter() : null
    );

    final UserSession userSession = connection.getSession();
    try (SchemaTreeProvider schemaTreeProvider = new SchemaTreeProvider(bee.getContext())) {
      final InfoSchemaTableType tableType = InfoSchemaTableType.COLUMNS;

      final PojoRecordReader<Column> records = (PojoRecordReader<Column>)
          tableType.getRecordReader(
              schemaTreeProvider.getRootSchema(userSession.getCredentials().getUserName(), provider),
              filter, userSession.getOptions());

      for(Column c : records) {
        final UserProtos.ColumnMetadata.Builder columnBuilder = UserProtos.ColumnMetadata.newBuilder();
        columnBuilder.setCatalogName(c.TABLE_CATALOG);
        columnBuilder.setSchemaName(c.TABLE_SCHEMA);
        columnBuilder.setTableName(c.TABLE_NAME);
        columnBuilder.setColumnName(c.COLUMN_NAME);
        columnBuilder.setOrdinalPosition(c.ORDINAL_POSITION);
        if (c.COLUMN_DEFAULT != null) {
          columnBuilder.setDefaultValue(c.COLUMN_DEFAULT);
        }

        if ("YES".equalsIgnoreCase(c.IS_NULLABLE)) {
          columnBuilder.setIsNullable(true);
        } else {
          columnBuilder.setIsNullable(false);
        }
        columnBuilder.setDataType(c.DATA_TYPE);
        if (c.CHARACTER_MAXIMUM_LENGTH != null) {
          columnBuilder.setCharMaxLength(c.CHARACTER_MAXIMUM_LENGTH);
        }

        if (c.CHARACTER_OCTET_LENGTH != null) {
          columnBuilder.setCharOctetLength(c.CHARACTER_OCTET_LENGTH);
        }

        if (c.NUMERIC_PRECISION != null) {
          columnBuilder.setNumericPrecision(c.NUMERIC_PRECISION);
        }

        if (c.NUMERIC_PRECISION_RADIX != null) {
          columnBuilder.setNumericPrecisionRadix(c.NUMERIC_PRECISION_RADIX);
        }

        if (c.DATETIME_PRECISION != null) {
          columnBuilder.setDateTimePrecision(c.DATETIME_PRECISION);
        }

        if (c.INTERVAL_TYPE != null) {
          columnBuilder.setIntervalType(c.INTERVAL_TYPE);
        }

        if (c.INTERVAL_PRECISION != null) {
          columnBuilder.setIntervalPrecision(c.INTERVAL_PRECISION);
        }

        respBuilder.addColumns(columnBuilder.build());
      }

      respBuilder.setStatus(RequestStatus.OK);
    } catch (Exception e) {
      respBuilder.setStatus(RequestStatus.FAILED);
      respBuilder.setError(createPBError("get columns", e));
    }

    return respBuilder.build();
  }

  private static InfoSchemaFilter createInfoSchemaFilter(final LikeFilter catalogNameFilter,
      final LikeFilter schemaNameFilter, final LikeFilter tableNameFilter, final LikeFilter columnNameFilter) {

    FunctionExprNode exprNode = null;
    if (catalogNameFilter != null) {
      exprNode = createFunctionExprNode(CATS_COL_CATALOG_NAME,  catalogNameFilter);
    }

    if (schemaNameFilter != null) {
      exprNode = combineFunctions(AND_FUNCTION,
          exprNode,
          combineFunctions(OR_FUNCTION,
              createFunctionExprNode(SHRD_COL_TABLE_SCHEMA, schemaNameFilter),
              createFunctionExprNode(SCHS_COL_SCHEMA_NAME, schemaNameFilter)
          )
      );
    }

    if (tableNameFilter != null) {
      exprNode = combineFunctions(AND_FUNCTION,
          exprNode,
          createFunctionExprNode(SHRD_COL_TABLE_NAME, tableNameFilter)
      );
    }

    if (columnNameFilter != null) {
      exprNode = combineFunctions(AND_FUNCTION,
          exprNode,
          createFunctionExprNode(InfoSchemaConstants.COLS_COL_COLUMN_NAME, columnNameFilter)
      );
    }

    return exprNode != null ? new InfoSchemaFilter(exprNode) : null;
  }

  private static FunctionExprNode createFunctionExprNode(String fieldName, LikeFilter likeFilter) {
    if (likeFilter.hasEscape()) {
      return new FunctionExprNode(LIKE_FUNCTION,
          ImmutableList.of(
              new FieldExprNode(fieldName),
              new ConstantExprNode(likeFilter.getRegex()),
              new ConstantExprNode(likeFilter.getEscape())
          )
      );
    } else {
      return new FunctionExprNode(LIKE_FUNCTION,
          ImmutableList.of(
              new FieldExprNode(fieldName),
              new ConstantExprNode(likeFilter.getRegex())
          )
      );
    }
  }

  private static FunctionExprNode combineFunctions(final String functionName,
      final FunctionExprNode func1, final FunctionExprNode func2) {
    if (func1 == null) {
      return func2;
    }

    if (func2 == null) {
      return func1;
    }

    return new FunctionExprNode(functionName, ImmutableList.<ExprNode>of(func1, func2));
  }

  private static DrillPBError createPBError(final String failedFunction, final Exception ex) {
    final String errorId = UUID.randomUUID().toString();
    logger.error("Failed to {}. ErrorId: {}", failedFunction, errorId, ex);

    final DrillPBError.Builder builder = DrillPBError.newBuilder();
    builder.setErrorType(ErrorType.SYSTEM); // Metadata requests shouldn't cause any user errors
    builder.setErrorId(errorId);
    if (ex.getMessage() != null) {
      builder.setMessage(ex.getMessage());
    }

    builder.setException(ErrorHelper.getWrapper(ex));

    return builder.build();
  }
}
