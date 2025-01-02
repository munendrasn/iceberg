/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.aws.dynamodb;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergViewException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.view.BaseMetastoreViewCatalog;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateGlobalSecondaryIndexAction;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.DeleteGlobalSecondaryIndexAction;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexUpdate;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;

/** DynamoDB implementation of Iceberg catalog */
public class DynamoDbCatalog extends BaseMetastoreViewCatalog
    implements SupportsNamespaces, Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoDbCatalog.class);
  private static final int CATALOG_TABLE_CREATION_WAIT_ATTEMPTS_MAX = 5;
  static final Joiner COMMA = Joiner.on(',');

  private static final String GSI_NAMESPACE_IDENTIFIER = "namespace-identifier";
  private static final String COL_IDENTIFIER = "identifier";
  private static final String COL_IDENTIFIER_NAMESPACE = "NAMESPACE";
  private static final String COL_NAMESPACE = "namespace";
  private static final String COL_ICEBERG_TYPE = "iceberg_type";
  private static final String PROPERTY_COL_PREFIX = "p.";
  private static final String PROPERTY_DEFAULT_LOCATION = "default_location";
  private static final String COL_CREATED_AT = "created_at";
  private static final String COL_UPDATED_AT = "updated_at";
  static final String TABLE_TYPE = "TABLE";
  static final String VIEW_TYPE = "VIEW";

  // field used for optimistic locking
  static final String COL_VERSION = "v";

  private DynamoDbClient dynamo;
  private Configuration hadoopConf;
  private String catalogName;
  private String warehousePath;
  private AwsProperties awsProperties;
  private FileIO fileIO;
  private CloseableGroup closeableGroup;
  private Map<String, String> catalogProperties;

  public DynamoDbCatalog() {}

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogProperties = ImmutableMap.copyOf(properties);
    initialize(
        name,
        properties.get(CatalogProperties.WAREHOUSE_LOCATION),
        new AwsProperties(properties),
        AwsClientFactories.from(properties).dynamo(),
        initializeFileIO(properties));
  }

  @VisibleForTesting
  void initialize(
      String name, String path, AwsProperties properties, DynamoDbClient client, FileIO io) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(path),
        "Cannot initialize DynamoDbCatalog because warehousePath must not be null or empty");

    this.catalogName = name;
    this.awsProperties = properties;
    this.warehousePath = LocationUtil.stripTrailingSlash(path);
    this.dynamo = client;
    this.fileIO = io;

    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(dynamo);
    closeableGroup.addCloseable(fileIO);
    closeableGroup.addCloseable(metricsReporter());
    closeableGroup.setSuppressCloseFailure(true);

    ensureCatalogTableExistsOrCreate();
    updateCatalogTableIfRequired();
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    validateTableIdentifier(tableIdentifier);
    return new DynamoDbTableOperations(dynamo, awsProperties, catalogName, fileIO, tableIdentifier);
  }

  @Override
  protected ViewOperations newViewOps(TableIdentifier viewIdentifier) {
    validateTableIdentifier(viewIdentifier);
    return new DynamoDbViewOperations(dynamo, awsProperties, catalogName, fileIO, viewIdentifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    validateTableIdentifier(tableIdentifier);
    GetItemResponse response = getItem(namespacePrimaryKey(tableIdentifier.namespace()));

    if (!response.hasItem()) {
      throw new NoSuchNamespaceException(
          "Cannot find default warehouse location: namespace %s does not exist",
          tableIdentifier.namespace());
    }

    String defaultLocationCol = toPropertyCol(PROPERTY_DEFAULT_LOCATION);
    if (response.item().containsKey(defaultLocationCol)) {
      return String.format(
          "%s/%s", response.item().get(defaultLocationCol).s(), tableIdentifier.name());
    } else {
      return String.format(
          "%s/%s.db/%s", warehousePath, tableIdentifier.namespace(), tableIdentifier.name());
    }
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    validateNamespace(namespace);
    Map<String, AttributeValue> values = namespacePrimaryKey(namespace);
    setNewCatalogEntryMetadata(values);
    addCatalogEntryMetadata(metadata, values);

    try {
      dynamo.putItem(
          PutItemRequest.builder()
              .tableName(awsProperties.dynamoDbTableName())
              .conditionExpression("attribute_not_exists(" + DynamoDbCatalog.COL_VERSION + ")")
              .item(values)
              .build());
    } catch (ConditionalCheckFailedException e) {
      throw new AlreadyExistsException("Cannot create namespace %s: already exists", namespace);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    validateNamespace(namespace);
    List<Namespace> namespaces = Lists.newArrayList();
    Map<String, AttributeValue> lastEvaluatedKey = null;
    String condition = COL_IDENTIFIER + " = :identifier";
    Map<String, AttributeValue> conditionValues = Maps.newHashMap();
    conditionValues.put(
        ":identifier", AttributeValue.builder().s(COL_IDENTIFIER_NAMESPACE).build());
    if (!namespace.isEmpty()) {
      condition += " AND " + "begins_with(" + COL_NAMESPACE + ",:ns)";
      conditionValues.put(":ns", AttributeValue.builder().s(namespace.toString()).build());
    }

    do {
      QueryResponse response =
          dynamo.query(
              QueryRequest.builder()
                  .tableName(awsProperties.dynamoDbTableName())
                  .consistentRead(true)
                  .keyConditionExpression(condition)
                  .expressionAttributeValues(conditionValues)
                  .exclusiveStartKey(lastEvaluatedKey)
                  .build());

      if (response.hasItems()) {
        for (Map<String, AttributeValue> item : response.items()) {
          String ns = item.get(COL_NAMESPACE).s();
          namespaces.add(Namespace.of(ns.split("\\.")));
        }
      }

      lastEvaluatedKey = response.lastEvaluatedKey();
    } while (!lastEvaluatedKey.isEmpty());

    return namespaces;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    validateNamespace(namespace);
    GetItemResponse response = getItem(namespacePrimaryKey(namespace));

    if (!response.hasItem()) {
      throw new NoSuchNamespaceException("Cannot find namespace %s", namespace);
    }

    return response.item().entrySet().stream()
        .filter(e -> isProperty(e.getKey()))
        .collect(Collectors.toMap(e -> toPropertyKey(e.getKey()), e -> e.getValue().s()));
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    validateNamespace(namespace);
    if (!listTables(namespace).isEmpty()) {
      throw new NamespaceNotEmptyException("Cannot delete non-empty namespace %s", namespace);
    }

    try {
      dynamo.deleteItem(
          DeleteItemRequest.builder()
              .tableName(awsProperties.dynamoDbTableName())
              .key(namespacePrimaryKey(namespace))
              .conditionExpression("attribute_exists(" + COL_NAMESPACE + ")")
              .build());
      return true;
    } catch (ConditionalCheckFailedException e) {
      return false;
    }
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    List<String> updateParts = Lists.newArrayList();
    Map<String, String> attributeNames = Maps.newHashMap();
    Map<String, AttributeValue> attributeValues = Maps.newHashMap();
    int idx = 0;
    for (Map.Entry<String, String> property : properties.entrySet()) {
      String attributeValue = ":v" + idx;
      String attributeKey = "#k" + idx;
      idx++;
      updateParts.add(attributeKey + " = " + attributeValue);
      attributeNames.put(attributeKey, toPropertyCol(property.getKey()));
      attributeValues.put(attributeValue, AttributeValue.builder().s(property.getValue()).build());
    }

    updateCatalogEntryMetadata(updateParts, attributeValues);
    String updateExpression = "SET " + COMMA.join(updateParts);
    return updateProperties(namespace, updateExpression, attributeValues, attributeNames);
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    List<String> removeParts = Lists.newArrayList(properties.iterator());
    Map<String, String> attributeNames = Maps.newHashMap();
    Map<String, AttributeValue> attributeValues = Maps.newHashMap();
    int idx = 0;
    for (String property : properties) {
      String attributeKey = "#k" + idx;
      idx++;
      removeParts.add(attributeKey);
      attributeNames.put(attributeKey, toPropertyCol(property));
    }

    List<String> updateParts = Lists.newArrayList();
    updateCatalogEntryMetadata(updateParts, attributeValues);
    String updateExpression =
        "REMOVE " + COMMA.join(removeParts) + " SET " + COMMA.join(updateParts);
    return updateProperties(namespace, updateExpression, attributeValues, attributeNames);
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return listEntities(namespace, DynamoDbCatalog::isIcebergTable);
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    Map<String, AttributeValue> key = tablePrimaryKey(identifier);
    try {
      GetItemResponse response = getItem(key);

      if (!response.hasItem()) {
        throw new NoSuchTableException("Cannot find table %s to drop", identifier);
      } else if (!isIcebergTable(response.item())) {
        throw new NoSuchTableException("Cannot find iceberg table %s to drop", identifier);
      }

      TableOperations ops = newTableOps(identifier);
      TableMetadata lastMetadata = null;
      if (purge) {
        try {
          lastMetadata = ops.current();
        } catch (NotFoundException e) {
          LOG.warn(
              "Failed to load table metadata for table: {}, continuing drop without purge",
              identifier,
              e);
        }
      }
      dynamo.deleteItem(
          DeleteItemRequest.builder()
              .tableName(awsProperties.dynamoDbTableName())
              .key(tablePrimaryKey(identifier))
              .conditionExpression(COL_VERSION + " = :v")
              .expressionAttributeValues(ImmutableMap.of(":v", response.item().get(COL_VERSION)))
              .build());
      LOG.info("Successfully dropped table {} from DynamoDb catalog", identifier);

      if (purge && lastMetadata != null) {
        CatalogUtil.dropTableData(ops.io(), lastMetadata);
        LOG.info("Table {} data purged", identifier);
      }

      LOG.info("Dropped table: {}", identifier);
      return true;
    } catch (ConditionalCheckFailedException e) {
      LOG.error("Cannot complete drop table operation for {}: commit conflict", identifier, e);
      return false;
    } catch (Exception e) {
      LOG.error("Cannot complete drop table operation for {}: unexpected exception", identifier, e);
      throw e;
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    Map<String, AttributeValue> fromKey = tablePrimaryKey(from);
    Map<String, AttributeValue> toKey = tablePrimaryKey(to);

    GetItemResponse fromResponse = getItem(fromKey);

    if (!fromResponse.hasItem()) {
      throw new NoSuchTableException(
          "Cannot rename table %s to %s: %s does not exist", from, to, from);
    } else if (isIcebergView(fromResponse.item())) {
      throw new ValidationException("Cannot rename table %s to %s: %s is a view", from, to, from);
    }

    GetItemResponse toResponse = getItem(toKey);

    if (toResponse.hasItem()) {
      throw new AlreadyExistsException(
          "Cannot rename table %s to %s: %s already exists", from, to, to);
    }

    renameTableOrView(fromKey, toKey, fromResponse, TABLE_TYPE);
    LOG.info("Successfully renamed table from {} to {}", from, to);
  }

  private void renameTableOrView(
      Map<String, AttributeValue> fromKey,
      Map<String, AttributeValue> toKey,
      GetItemResponse fromResponse,
      String icebergType) {
    fromResponse.item().entrySet().stream()
        .filter(e -> isProperty(e.getKey()))
        .forEach(e -> toKey.put(e.getKey(), e.getValue()));

    setNewCatalogEntryMetadataWithType(toKey, icebergType);

    dynamo.transactWriteItems(
        TransactWriteItemsRequest.builder()
            .transactItems(
                TransactWriteItem.builder()
                    .delete(
                        Delete.builder()
                            .tableName(awsProperties.dynamoDbTableName())
                            .key(fromKey)
                            .conditionExpression(COL_VERSION + " = :v")
                            .expressionAttributeValues(
                                ImmutableMap.of(":v", fromResponse.item().get(COL_VERSION)))
                            .build())
                    .build(),
                TransactWriteItem.builder()
                    .put(
                        Put.builder()
                            .tableName(awsProperties.dynamoDbTableName())
                            .item(toKey)
                            .conditionExpression("attribute_not_exists(" + COL_VERSION + ")")
                            .build())
                    .build())
            .build());
  }

  private List<TableIdentifier> listEntities(
      Namespace namespace, Function<Map<String, AttributeValue>, Boolean> typeEvaluator) {
    List<TableIdentifier> identifiers = Lists.newArrayList();
    Map<String, AttributeValue> lastEvaluatedKey = null;
    String condition = COL_NAMESPACE + " = :ns";
    Map<String, AttributeValue> conditionValues =
        ImmutableMap.of(":ns", AttributeValue.builder().s(namespace.toString()).build());
    // todo: use page iterator, and filter by type on dynamo
    do {
      QueryResponse response =
          dynamo.query(
              QueryRequest.builder()
                  .tableName(awsProperties.dynamoDbTableName())
                  .indexName(GSI_NAMESPACE_IDENTIFIER)
                  .keyConditionExpression(condition)
                  .expressionAttributeValues(conditionValues)
                  .exclusiveStartKey(lastEvaluatedKey)
                  .build());

      if (response.hasItems()) {
        for (Map<String, AttributeValue> item : response.items()) {
          String identifier = item.get(COL_IDENTIFIER).s();
          if (!COL_IDENTIFIER_NAMESPACE.equals(identifier) && typeEvaluator.apply(item)) {
            identifiers.add(TableIdentifier.of(identifier.split("\\.")));
          }
        }
      }

      lastEvaluatedKey = response.lastEvaluatedKey();
    } while (!lastEvaluatedKey.isEmpty());
    return identifiers;
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    return listEntities(namespace, DynamoDbCatalog::isIcebergView);
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    Map<String, AttributeValue> key = tablePrimaryKey(identifier);
    try {
      GetItemResponse response = getItem(key);

      if (!response.hasItem()) {
        throw new NoSuchViewException("Cannot find view %s to drop", identifier);
      } else if (!isIcebergView(response.item())) {
        throw new NoSuchIcebergViewException("Cannot find iceberg view %s to drop", identifier);
      }
      DynamoDbViewOperations ops = (DynamoDbViewOperations) newViewOps(identifier);
      ViewMetadata lastViewMetadata = null;
      try {
        lastViewMetadata = ops.current();
      } catch (NotFoundException e) {
        LOG.warn("Failed to load view metadata for view: {}", identifier, e);
      }

      dynamo.deleteItem(
          DeleteItemRequest.builder()
              .tableName(awsProperties.dynamoDbTableName())
              .key(tablePrimaryKey(identifier))
              .conditionExpression(COL_VERSION + " = :v")
              .expressionAttributeValues(ImmutableMap.of(":v", response.item().get(COL_VERSION)))
              .build());

      if (lastViewMetadata != null) {
        CatalogUtil.dropViewMetadata(ops.io(), lastViewMetadata);
      }
      LOG.info("Successfully dropped view {} from DynamoDb catalog", identifier);
      return true;
    } catch (ConditionalCheckFailedException e) {
      LOG.error("Cannot complete drop view operation for {}: commit conflict", identifier, e);
      return false;
    } catch (Exception e) {
      LOG.error("Cannot complete drop view operation for {}: unexpected exception", identifier, e);
      throw e;
    }
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    Map<String, AttributeValue> fromKey = tablePrimaryKey(from);
    Map<String, AttributeValue> toKey = tablePrimaryKey(to);

    GetItemResponse fromResponse = getItem(fromKey);

    if (!fromResponse.hasItem()) {
      throw new NoSuchViewException(
          "Cannot rename view %s to %s: %s does not exist", from, to, from);
    } else if (isIcebergTable(fromResponse.item())) {
      throw new ValidationException("Cannot rename view %s to %s: %s is a table", from, to, from);
    }

    GetItemResponse toResponse = getItem(toKey);

    if (toResponse.hasItem()) {
      throw new AlreadyExistsException(
          "Cannot rename view %s to %s: %s already exists", from, to, to);
    }

    renameTableOrView(fromKey, toKey, fromResponse, VIEW_TYPE);

    LOG.info("Successfully renamed view from {} to {}", from, to);
  }

  @Override
  public void setConf(Configuration conf) {
    hadoopConf = conf;
  }

  @Override
  public Configuration getConf() {
    return hadoopConf;
  }

  @Override
  public void close() throws IOException {
    closeableGroup.close();
  }

  private GetItemResponse getItem(Map<String, AttributeValue> key) {
    return dynamo.getItem(
        GetItemRequest.builder()
            .tableName(awsProperties.dynamoDbTableName())
            .consistentRead(true)
            .key(key)
            .build());
  }

  /**
   * The property used to set a default location for tables in a namespace. Call {@link
   * #setProperties(Namespace, Map)} to set a path value using this property for a namespace, then
   * all tables in the namespace will have default table root path under that given path.
   *
   * @return default location property key
   */
  public static String defaultLocationProperty() {
    return PROPERTY_DEFAULT_LOCATION;
  }

  static String toPropertyCol(String propertyKey) {
    return PROPERTY_COL_PREFIX + propertyKey;
  }

  static boolean isProperty(String dynamoCol) {
    return dynamoCol.startsWith(PROPERTY_COL_PREFIX);
  }

  static String toPropertyKey(String propertyCol) {
    return propertyCol.substring(PROPERTY_COL_PREFIX.length());
  }

  static boolean isIcebergView(Map<String, AttributeValue> values) {
    return values.containsKey(COL_ICEBERG_TYPE)
        && VIEW_TYPE.equalsIgnoreCase(values.get(COL_ICEBERG_TYPE).s());
  }

  static boolean isIcebergTable(Map<String, AttributeValue> values) {
    return !values.containsKey(COL_ICEBERG_TYPE)
        || (values.containsKey(COL_ICEBERG_TYPE)
            && TABLE_TYPE.equalsIgnoreCase(values.get(COL_ICEBERG_TYPE).s()));
  }

  static Map<String, AttributeValue> namespacePrimaryKey(Namespace namespace) {
    Map<String, AttributeValue> key = Maps.newHashMap();
    key.put(COL_IDENTIFIER, AttributeValue.builder().s(COL_IDENTIFIER_NAMESPACE).build());
    key.put(COL_NAMESPACE, AttributeValue.builder().s(namespace.toString()).build());
    return key;
  }

  static Map<String, AttributeValue> tablePrimaryKey(TableIdentifier identifier) {
    Map<String, AttributeValue> key = Maps.newHashMap();
    key.put(COL_IDENTIFIER, AttributeValue.builder().s(identifier.toString()).build());
    key.put(COL_NAMESPACE, AttributeValue.builder().s(identifier.namespace().toString()).build());
    return key;
  }

  static void addCatalogEntryMetadata(
      Map<String, String> parameters, Map<String, AttributeValue> values) {
    parameters.forEach(
        (k, v) -> values.put(toPropertyCol(k), AttributeValue.builder().s(v).build()));
  }

  static void setNewCatalogEntryMetadata(Map<String, AttributeValue> values) {
    String current = Long.toString(System.currentTimeMillis());
    values.put(COL_CREATED_AT, AttributeValue.builder().n(current).build());
    values.put(COL_UPDATED_AT, AttributeValue.builder().n(current).build());
    values.put(COL_VERSION, AttributeValue.builder().s(UUID.randomUUID().toString()).build());
  }

  static void setNewCatalogEntryMetadataWithType(
      Map<String, AttributeValue> values, String icebergType) {
    setNewCatalogEntryMetadata(values);
    values.put(COL_ICEBERG_TYPE, AttributeValue.builder().s(icebergType).build());
  }

  static void updateCatalogEntryMetadata(
      List<String> updateParts, Map<String, AttributeValue> attributeValues) {
    updateParts.add(COL_UPDATED_AT + " = :uat");
    attributeValues.put(
        ":uat", AttributeValue.builder().n(Long.toString(System.currentTimeMillis())).build());
    updateParts.add(COL_VERSION + " = :uv");
    attributeValues.put(":uv", AttributeValue.builder().s(UUID.randomUUID().toString()).build());
  }

  private FileIO initializeFileIO(Map<String, String> properties) {
    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    if (fileIOImpl == null) {
      FileIO io = new S3FileIO();
      io.initialize(properties);
      return io;
    } else {
      return CatalogUtil.loadFileIO(fileIOImpl, properties, hadoopConf);
    }
  }

  private void validateNamespace(Namespace namespace) {
    for (String level : namespace.levels()) {
      ValidationException.check(
          level != null && !level.isEmpty(), "Namespace level must not be empty: %s", namespace);
      ValidationException.check(
          !level.contains("."),
          "Namespace level must not contain dot, but found %s in %s",
          level,
          namespace);
    }
  }

  private void validateTableIdentifier(TableIdentifier identifier) {
    validateNamespace(identifier.namespace());
    ValidationException.check(
        identifier.hasNamespace(), "Table namespace must not be empty: %s", identifier);
    String tableName = identifier.name();
    ValidationException.check(
        !tableName.contains("."), "Table name must not contain dot: %s", tableName);
  }

  private boolean dynamoDbTableExists(String tableName) {
    try {
      dynamo.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
      return true;
    } catch (ResourceNotFoundException e) {
      return false;
    }
  }

  private void updateCatalogTableIfRequired() {
    if (!awsProperties.updateDynamoDbCatalogTable()) {
      LOG.info(
          "Skipping updating the Dynamo table {} as update is disabled",
          awsProperties.dynamoDbTableName());
      return;
    }
    DescribeTableResponse describeTableResponse =
        dynamo.describeTable(
            DescribeTableRequest.builder().tableName(awsProperties.dynamoDbTableName()).build());
    Optional<GlobalSecondaryIndexDescription> namespaceIdentifierGsi =
        describeTableResponse.table().globalSecondaryIndexes().stream()
            .filter(index -> GSI_NAMESPACE_IDENTIFIER.equals(index.indexName()))
            .findFirst();
    boolean doesNotContainIcebergType = true;
    if (namespaceIdentifierGsi.isPresent()) {
      doesNotContainIcebergType =
          !namespaceIdentifierGsi.get().projection().hasNonKeyAttributes()
              || !namespaceIdentifierGsi
                  .get()
                  .projection()
                  .nonKeyAttributes()
                  .contains(COL_ICEBERG_TYPE);
    }

    if (!doesNotContainIcebergType) {
      LOG.info(
          "GSI {} contains for table {} attribute {} in projection, skipping update",
          GSI_NAMESPACE_IDENTIFIER,
          awsProperties.dynamoDbTableName(),
          COL_ICEBERG_TYPE);
      return;
    }
    if (namespaceIdentifierGsi.isPresent()) {
      LOG.info(
          "Deleting GSI {} for table {} to recreate the GSI with attribute {}",
          GSI_NAMESPACE_IDENTIFIER,
          awsProperties.dynamoDbTableName(),
          COL_ICEBERG_TYPE);
      dynamo.updateTable(
          UpdateTableRequest.builder()
              .tableName(awsProperties.dynamoDbTableName())
              .globalSecondaryIndexUpdates(
                  GlobalSecondaryIndexUpdate.builder()
                      .delete(
                          DeleteGlobalSecondaryIndexAction.builder()
                              .indexName(GSI_NAMESPACE_IDENTIFIER)
                              .build())
                      .build())
              .build());
    }
    // Not checking the GSI being Active as back-fill would take more time
    dynamo.updateTable(
        UpdateTableRequest.builder()
            .tableName(awsProperties.dynamoDbTableName())
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName(COL_IDENTIFIER)
                    .attributeType(ScalarAttributeType.S)
                    .build(),
                AttributeDefinition.builder()
                    .attributeName(COL_NAMESPACE)
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .globalSecondaryIndexUpdates(
                GlobalSecondaryIndexUpdate.builder()
                    .create(
                        CreateGlobalSecondaryIndexAction.builder()
                            .indexName(GSI_NAMESPACE_IDENTIFIER)
                            .keySchema(
                                KeySchemaElement.builder()
                                    .attributeName(COL_NAMESPACE)
                                    .keyType(KeyType.HASH)
                                    .build(),
                                KeySchemaElement.builder()
                                    .attributeName(COL_IDENTIFIER)
                                    .keyType(KeyType.RANGE)
                                    .build())
                            .projection(
                                Projection.builder()
                                    .projectionType(ProjectionType.INCLUDE)
                                    .nonKeyAttributes(COL_ICEBERG_TYPE)
                                    .build())
                            .build())
                    .build())
            .build());
  }

  private void ensureCatalogTableExistsOrCreate() {
    if (dynamoDbTableExists(awsProperties.dynamoDbTableName())) {
      return;
    }

    LOG.info(
        "DynamoDb catalog table {} not found, trying to create", awsProperties.dynamoDbTableName());
    dynamo.createTable(
        CreateTableRequest.builder()
            .tableName(awsProperties.dynamoDbTableName())
            .keySchema(
                KeySchemaElement.builder()
                    .attributeName(COL_IDENTIFIER)
                    .keyType(KeyType.HASH)
                    .build(),
                KeySchemaElement.builder()
                    .attributeName(COL_NAMESPACE)
                    .keyType(KeyType.RANGE)
                    .build())
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName(COL_IDENTIFIER)
                    .attributeType(ScalarAttributeType.S)
                    .build(),
                AttributeDefinition.builder()
                    .attributeName(COL_NAMESPACE)
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .globalSecondaryIndexes(
                GlobalSecondaryIndex.builder()
                    .indexName(GSI_NAMESPACE_IDENTIFIER)
                    .keySchema(
                        KeySchemaElement.builder()
                            .attributeName(COL_NAMESPACE)
                            .keyType(KeyType.HASH)
                            .build(),
                        KeySchemaElement.builder()
                            .attributeName(COL_IDENTIFIER)
                            .keyType(KeyType.RANGE)
                            .build())
                    .projection(
                        Projection.builder()
                            .projectionType(ProjectionType.INCLUDE)
                            .nonKeyAttributes(COL_ICEBERG_TYPE)
                            .build())
                    .build())
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .build());

    // wait for the dynamo table to complete provisioning, which takes around 10 seconds
    Tasks.foreach(awsProperties.dynamoDbTableName())
        .retry(CATALOG_TABLE_CREATION_WAIT_ATTEMPTS_MAX)
        .throwFailureWhenFinished()
        .onlyRetryOn(IllegalStateException.class)
        .run(this::checkTableActive);
  }

  private void checkTableActive(String tableName) {
    try {
      DescribeTableResponse response =
          dynamo.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
      TableStatus currentStatus = response.table().tableStatus();
      if (!currentStatus.equals(TableStatus.ACTIVE)) {
        throw new IllegalStateException(
            String.format(
                "Dynamo catalog table %s is not active, current status: %s",
                tableName, currentStatus));
      }
    } catch (ResourceNotFoundException e) {
      throw new IllegalStateException(
          String.format("Cannot find Dynamo catalog table %s", tableName));
    }
  }

  private boolean updateProperties(
      Namespace namespace,
      String updateExpression,
      Map<String, AttributeValue> attributeValues,
      Map<String, String> attributeNames) {
    validateNamespace(namespace);
    Map<String, AttributeValue> key = namespacePrimaryKey(namespace);
    try {
      GetItemResponse response =
          dynamo.getItem(
              GetItemRequest.builder()
                  .tableName(awsProperties.dynamoDbTableName())
                  .consistentRead(true)
                  .key(key)
                  .build());

      if (!response.hasItem()) {
        throw new NoSuchNamespaceException("Cannot find namespace %s", namespace);
      }

      attributeValues.put(":v", response.item().get(COL_VERSION));
      dynamo.updateItem(
          UpdateItemRequest.builder()
              .tableName(awsProperties.dynamoDbTableName())
              .key(key)
              .conditionExpression(COL_VERSION + " = :v")
              .updateExpression(updateExpression)
              .expressionAttributeValues(attributeValues)
              .expressionAttributeNames(attributeNames)
              .build());
      return true;
    } catch (ConditionalCheckFailedException e) {
      return false;
    }
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }
}
