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

import java.util.Map;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.util.RetryDetector;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

class DynamoDbTableOperations extends BaseMetastoreTableOperations
    implements DynamoDbOperationsBase {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoDbTableOperations.class);

  private final DynamoDbClient dynamo;
  private final AwsProperties awsProperties;
  private final TableIdentifier tableIdentifier;
  private final String fullTableName;
  private final FileIO fileIO;

  DynamoDbTableOperations(
      DynamoDbClient dynamo,
      AwsProperties awsProperties,
      String catalogName,
      FileIO fileIO,
      TableIdentifier tableIdentifier) {
    this.dynamo = dynamo;
    this.awsProperties = awsProperties;
    this.fullTableName = String.format("%s.%s", catalogName, tableIdentifier);
    this.tableIdentifier = tableIdentifier;
    this.fileIO = fileIO;
  }

  @Override
  protected String tableName() {
    return fullTableName;
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    GetItemResponse table = getTableItem(tableIdentifier);
    if (table.hasItem() && DynamoDbCatalog.isIcebergTable(table.item())) {
      metadataLocation = DynamoDbOperationsBase.getMetadataLocation(table);
    } else {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(
            "Cannot find table %s after refresh, "
                + "maybe another process deleted it or revoked your access permission",
            tableName());
      }
    }

    refreshFromMetadataLocation(metadataLocation);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    boolean newTable = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);
    CommitStatus commitStatus = CommitStatus.FAILURE;
    RetryDetector retryDetector = new RetryDetector();
    Map<String, AttributeValue> tableKey = DynamoDbCatalog.tablePrimaryKey(tableIdentifier);
    try {
      GetItemResponse table = getTableItem(tableIdentifier);
      checkMetadataLocation(table, base);
      Map<String, String> properties = prepareProperties(table, newMetadataLocation);
      persistTable(tableKey, table, properties, retryDetector);
      commitStatus = CommitStatus.SUCCESS;
    } catch (CommitFailedException e) {
      // any explicit commit failures are passed up and out to the retry handler
      throw e;
    } catch (RuntimeException persistFailure) {
      boolean conditionCheckFailed = persistFailure instanceof ConditionalCheckFailedException;

      // If we got an exception we weren't expecting, or we got a ConditionalCheckFailedException
      // but retries were performed, attempt to reconcile the actual commit status.
      if (!conditionCheckFailed || retryDetector.retried()) {
        LOG.warn(
            "Received unexpected failure when committing to {}, validating if commit ended up succeeding.",
            fullTableName,
            persistFailure);
        commitStatus = checkCommitStatus(newMetadataLocation, metadata);
      }

      if (commitStatus != CommitStatus.SUCCESS && conditionCheckFailed) {
        throw new CommitFailedException(
            persistFailure, "Cannot commit %s: concurrent update detected", tableName());
      }

      switch (commitStatus) {
        case SUCCESS:
          break;
        case FAILURE:
          throw new CommitFailedException(
              persistFailure, "Cannot commit %s due to unexpected exception", tableName());
        case UNKNOWN:
          throw new CommitStateUnknownException(persistFailure);
      }
    } finally {
      try {
        if (commitStatus == CommitStatus.FAILURE) {
          // if anything went wrong, clean up the uncommitted metadata file
          io().deleteFile(newMetadataLocation);
        }
      } catch (RuntimeException e) {
        LOG.error("Failed to cleanup metadata file at {}", newMetadataLocation, e);
      }
    }
  }

  private void checkMetadataLocation(GetItemResponse table, TableMetadata base) {
    String dynamoMetadataLocation =
        table.hasItem() ? DynamoDbOperationsBase.getMetadataLocation(table) : null;
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
    checkMetadataLocation(baseMetadataLocation, dynamoMetadataLocation);
  }

  private Map<String, String> prepareProperties(
      GetItemResponse response, String newMetadataLocation) {
    return DynamoDbOperationsBase.prepareProperties(
        response, newMetadataLocation, ICEBERG_TABLE_TYPE_VALUE, currentMetadataLocation());
  }

  @Override
  public String type() {
    return DynamoDbCatalog.TABLE_TYPE;
  }

  @Override
  public String name() {
    return tableName();
  }

  @Override
  public DynamoDbClient dynamo() {
    return dynamo;
  }

  @Override
  public AwsProperties awsProperties() {
    return awsProperties;
  }
}
