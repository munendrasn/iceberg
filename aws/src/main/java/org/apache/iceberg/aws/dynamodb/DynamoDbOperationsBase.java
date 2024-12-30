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

import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.util.RetryDetector;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

interface DynamoDbOperationsBase {

  static final Logger LOG = LoggerFactory.getLogger(DynamoDbOperationsBase.class);

  String type();

  String name();

  DynamoDbClient dynamo();

  AwsProperties awsProperties();

  default GetItemResponse getTableItem(TableIdentifier identifier) {
    Map<String, AttributeValue> tableKey = DynamoDbCatalog.tablePrimaryKey(identifier);
    return dynamo()
        .getItem(
            GetItemRequest.builder()
                .tableName(awsProperties().dynamoDbTableName())
                .consistentRead(true)
                .key(tableKey)
                .build());
  }

  default void checkMetadataLocation(String baseMetadataLocation, String dynamoMetadataLocation) {
    if (!Objects.equals(baseMetadataLocation, dynamoMetadataLocation)) {
      throw new CommitFailedException(
          "Cannot commit %s because base metadata location '%s' is not same as the current DynamoDb location '%s'",
          name(), baseMetadataLocation, dynamoMetadataLocation);
    }
  }

  default void persistTable(
      Map<String, AttributeValue> tableKey,
      GetItemResponse table,
      Map<String, String> parameters,
      RetryDetector retryDetector) {
    if (table.hasItem()) {
      LOG.debug("Committing existing DynamoDb catalog {}: {}", type(), name());
      List<String> updateParts = Lists.newArrayList();
      Map<String, String> attributeNames = Maps.newHashMap();
      Map<String, AttributeValue> attributeValues = Maps.newHashMap();
      int idx = 0;
      for (Map.Entry<String, String> property : parameters.entrySet()) {
        String attributeValue = ":v" + idx;
        String attributeKey = "#k" + idx;
        idx++;
        updateParts.add(attributeKey + " = " + attributeValue);
        attributeNames.put(attributeKey, DynamoDbCatalog.toPropertyCol(property.getKey()));
        attributeValues.put(
            attributeValue, AttributeValue.builder().s(property.getValue()).build());
      }
      DynamoDbCatalog.updateCatalogEntryMetadata(updateParts, attributeValues);
      String updateExpression = "SET " + DynamoDbCatalog.COMMA.join(updateParts);
      attributeValues.put(":v", table.item().get(DynamoDbCatalog.COL_VERSION));
      dynamo()
          .updateItem(
              UpdateItemRequest.builder()
                  .overrideConfiguration(c -> c.addMetricPublisher(retryDetector))
                  .tableName(awsProperties().dynamoDbTableName())
                  .key(tableKey)
                  .conditionExpression(DynamoDbCatalog.COL_VERSION + " = :v")
                  .updateExpression(updateExpression)
                  .expressionAttributeValues(attributeValues)
                  .expressionAttributeNames(attributeNames)
                  .build());
    } else {
      LOG.debug("Committing new DynamoDb catalog {}: {}", type(), name());
      Map<String, AttributeValue> values = Maps.newHashMap(tableKey);
      DynamoDbCatalog.addCatalogEntryMetadata(parameters, values);
      DynamoDbCatalog.setNewCatalogEntryMetadataWithType(values, type());

      dynamo()
          .putItem(
              PutItemRequest.builder()
                  .overrideConfiguration(c -> c.addMetricPublisher(retryDetector))
                  .tableName(awsProperties().dynamoDbTableName())
                  .item(values)
                  .conditionExpression("attribute_not_exists(" + DynamoDbCatalog.COL_VERSION + ")")
                  .build());
    }
  }

  static Map<String, String> prepareProperties(
      GetItemResponse response,
      String newMetadataLocation,
      String icebergType,
      String currentMetadataLocation) {
    Map<String, String> properties =
        response.hasItem() ? getProperties(response) : Maps.newHashMap();
    properties.put(
        BaseMetastoreTableOperations.TABLE_TYPE_PROP, icebergType.toUpperCase(Locale.ENGLISH));
    properties.put(METADATA_LOCATION_PROP, newMetadataLocation);
    if (currentMetadataLocation != null && !currentMetadataLocation.isEmpty()) {
      properties.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation);
    }
    return properties;
  }

  static Map<String, String> getProperties(GetItemResponse table) {
    return table.item().entrySet().stream()
        .filter(e -> DynamoDbCatalog.isProperty(e.getKey()))
        .collect(
            Collectors.toMap(
                e -> DynamoDbCatalog.toPropertyKey(e.getKey()), e -> e.getValue().s()));
  }

  static String getMetadataLocation(GetItemResponse table) {
    return table.item().get(DynamoDbCatalog.toPropertyCol(METADATA_LOCATION_PROP)).s();
  }
}
