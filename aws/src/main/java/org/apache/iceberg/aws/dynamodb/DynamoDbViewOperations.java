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
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.util.RetryDetector;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.ViewMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

class DynamoDbViewOperations extends BaseViewOperations implements DynamoDbOperationsBase {

  private static final Logger LOG = LoggerFactory.getLogger(DynamoDbViewOperations.class);
  private static final String ICEBERG_VIEW_TYPE_VALUE = "iceberg-view";

  private final DynamoDbClient dynamo;
  private final AwsProperties awsProperties;
  private final TableIdentifier viewIdentifier;
  private final String fullViewName;
  private final FileIO fileIO;

  DynamoDbViewOperations(
      DynamoDbClient dynamo,
      AwsProperties awsProperties,
      String catalogName,
      FileIO fileIO,
      TableIdentifier viewIdentifier) {
    this.dynamo = dynamo;
    this.awsProperties = awsProperties;
    this.fullViewName = String.format("%s.%s", catalogName, viewIdentifier);
    this.viewIdentifier = viewIdentifier;
    this.fileIO = fileIO;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    GetItemResponse table = getTableItem(viewIdentifier);
    if (table.hasItem() && DynamoDbCatalog.isIcebergView(table.item())) {
      metadataLocation = DynamoDbOperationsBase.getMetadataLocation(table);
    } else {
      if (currentMetadataLocation() != null) {
        throw new NoSuchViewException(
            "Cannot find view %s after refresh, "
                + "maybe another process deleted it or revoked your access permission",
            viewName());
      }
    }

    refreshFromMetadataLocation(metadataLocation);
  }

  @Override
  protected void doCommit(ViewMetadata base, ViewMetadata metadata) {
    String newMetadataLocation = writeNewMetadataIfRequired(metadata);
    CommitStatus commitStatus = CommitStatus.FAILURE;
    RetryDetector retryDetector = new RetryDetector();
    Map<String, AttributeValue> tableKey = DynamoDbCatalog.tablePrimaryKey(viewIdentifier);
    try {
      GetItemResponse table = getTableItem(viewIdentifier);
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
            fullViewName,
            persistFailure);
        commitStatus = checkCommitStatus(newMetadataLocation, metadata);
      }

      if (commitStatus != CommitStatus.SUCCESS && conditionCheckFailed) {
        throw new CommitFailedException(
            persistFailure, "Cannot commit %s: concurrent update detected", viewName());
      }

      switch (commitStatus) {
        case SUCCESS:
          break;
        case FAILURE:
          throw new CommitFailedException(
              persistFailure, "Cannot commit %s due to unexpected exception", viewName());
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

  private void checkMetadataLocation(GetItemResponse table, ViewMetadata base) {
    String dynamoMetadataLocation =
        table.hasItem() ? DynamoDbOperationsBase.getMetadataLocation(table) : null;
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
    checkMetadataLocation(baseMetadataLocation, dynamoMetadataLocation);
  }

  @Override
  protected String viewName() {
    return fullViewName;
  }

  @Override
  protected FileIO io() {
    return fileIO;
  }

  private Map<String, String> prepareProperties(
      GetItemResponse response, String newMetadataLocation) {
    return DynamoDbOperationsBase.prepareProperties(
        response, newMetadataLocation, ICEBERG_VIEW_TYPE_VALUE, currentMetadataLocation());
  }

  @Override
  public String type() {
    return DynamoDbCatalog.VIEW_TYPE;
  }

  @Override
  public String name() {
    return viewName();
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
