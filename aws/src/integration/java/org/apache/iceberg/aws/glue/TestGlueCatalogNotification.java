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

package org.apache.iceberg.aws.glue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.sqs.SQSListener; // import org.apache.iceberg.aws.sns.SNSListener;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.IncrementalScanEvent;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

public class TestGlueCatalogNotification extends GlueTestBase {
  @Test
  public void testNotifyOnCreateSnapshotEvent() throws IOException {
    List<Message> messages = getMessages(testCreateSnapshotQueue);
    clearQueue(messages, testCreateSnapshotQueue);
    messages = getMessages(testCreateSnapshotQueue);
    Assert.assertEquals(0, messages.size());

    // Listeners.register(new SNSListener(testARN, sns), CreateSnapshotEvent.class);
    Listeners.register(new SQSListener(testCreateSnapshotQueue, sqs), CreateSnapshotEvent.class);

    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));

    table.newAppend().appendFile(testDataFile).commit();

    messages = getMessages(testCreateSnapshotQueue);
    clearQueue(messages, testCreateSnapshotQueue);
    Assert.assertEquals(1, messages.size());

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode bodyNode = objectMapper.readTree(messages.get(0).body());

    String expectedMessage = "{\"table-name\":\"" + "glue." + namespace + "." + tableName + "\"," +
            "\"operation\":\"append\",\"snapshot-id\":" + table.currentSnapshot().snapshotId() + "," +
            "\"sequence-number\":0,\"summary\":{\"added-data-files\":\"1\"," +
            "\"added-records\":\"1\",\"added-files-size\":\"10\"," +
            "\"changed-partition-count\":\"1\",\"total-records\":\"1\"," +
            "\"total-files-size\":\"10\",\"total-data-files\":\"1\"," +
            "\"total-delete-files\":\"0\",\"total-position-deletes\":\"0\"," +
            "\"total-equality-deletes\":\"0\"}}";

    Assert.assertEquals(expectedMessage, bodyNode.toString());
  }

  @Test
  public void testNotifyOnScanEvent() throws IOException {
    List<Message> messages = getMessages(testScanQueue);
    clearQueue(messages, testScanQueue);
    messages = getMessages(testScanQueue);
    Assert.assertEquals(0, messages.size());

    // Listeners.register(new SNSListener(testARN, sns), ScanEvent.class);
    Listeners.register(new SQSListener(testScanQueue, sqs), ScanEvent.class);

    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));

    table.newAppend().appendFile(testDataFile).commit();
    table.refresh();

    Expression andExpression = Expressions.and(Expressions.equal("c1", "First"), Expressions.equal("c1", "Second"));
    table.newScan().filter(andExpression).planFiles();

    messages = getMessages(testScanQueue);
    clearQueue(messages, testScanQueue);
    Assert.assertEquals(1, messages.size());

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode bodyNode = objectMapper.readTree(messages.get(0).body());

    String expectedMessage = "{\"table-name\":\"" + "glue." + namespace + "." + tableName + "\"," +
            "\"snapshot-id\":" + table.currentSnapshot().snapshotId() + "," +
            "\"expression\":{\"type\":\"and\"," +
            "\"left-operand\":{\"type\":\"unbounded-predicate\"," +
            "\"operation\":\"eq\",\"term\":{\"type\":\"named-reference\",\"value\":\"c1\"}," +
            "\"literals\":[{\"type\":\"string\",\"value\":\"First\"}]}," +
            "\"right-operand\":{\"type\":\"unbounded-predicate\"," +
            "\"operation\":\"eq\",\"term\":{\"type\":\"named-reference\",\"value\":\"c1\"}," +
            "\"literals\":[{\"type\":\"string\",\"value\":\"Second\"}]}}," +
            "\"projection\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"c1\"," +
            "\"required\":true,\"type\":\"string\",\"doc\":\"c1\"}]}}";

    Assert.assertEquals(expectedMessage, bodyNode.toString());
  }

  @Test
  public void testNotifyOnIncrementalScan() throws IOException {
    List<Message> messages = getMessages(testIncrementalScanQueue);
    clearQueue(messages, testIncrementalScanQueue);
    messages = getMessages(testIncrementalScanQueue);
    Assert.assertEquals(0, messages.size());

    // Listeners.register(new SNSListener(testARN, sns), IncrementalScanEvent.class);
    Listeners.register(new SQSListener(testIncrementalScanQueue, sqs), IncrementalScanEvent.class);

    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));

    table.newAppend().appendFile(testDataFile).commit();
    table.newAppend().appendFile(testDataFile).commit();
    table.refresh();

    Iterable<Snapshot> snapshots = table.snapshots();
    table.newScan().appendsBetween(
            Iterables.get(snapshots, 0).snapshotId(),
            Iterables.get(snapshots, 1).snapshotId())
            .planFiles();

    messages = getMessages(testIncrementalScanQueue);
    clearQueue(messages, testIncrementalScanQueue);
    Assert.assertEquals(1, messages.size());

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode bodyNode = objectMapper.readTree(messages.get(0).body());

    String expectedMessage = "{\"table-name\":\"" + "glue." + namespace + "." + tableName + "\"," +
            "\"from-snapshot-id\":" + Iterables.get(snapshots, 0).snapshotId() + "," +
            "\"to-snapshot-id\":" + Iterables.get(snapshots, 1).snapshotId() + "," +
            "\"expression\":{\"type\":\"true\"}," +
            "\"projection\":{\"type\":\"struct\"," +
            "\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"c1\"," +
            "\"required\":true,\"type\":\"string\",\"doc\":\"c1\"}]}}";

    Assert.assertEquals(expectedMessage, bodyNode.toString());
  }

  @Test
  public void testNotifyOnAllEvents() throws IOException {
    List<Message> messages = getMessages(testAllEventQueue);
    clearQueue(messages, testAllEventQueue);

    messages = getMessages(testAllEventQueue);
    Assert.assertEquals(0, messages.size());

    // SNSListener snsListener = new SNSListener(testARN, sns);
    SQSListener sqsListener = new SQSListener(testAllEventQueue, sqs);
    Listeners.register(sqsListener, CreateSnapshotEvent.class);
    Listeners.register(sqsListener, ScanEvent.class);
    Listeners.register(sqsListener, IncrementalScanEvent.class);

    String namespace = createNamespace();
    String tableName = getRandomName();
    createTable(namespace, tableName);
    Table table = glueCatalog.loadTable(TableIdentifier.of(namespace, tableName));

    table.newAppend().appendFile(testDataFile).commit();
    table.newScan().planFiles();

    table.newAppend().appendFile(testDataFile).commit();
    table.refresh();

    Iterable<Snapshot> snapshots = table.snapshots();
    table.newScan().appendsBetween(
            Iterables.get(snapshots, 0).snapshotId(),
            Iterables.get(snapshots, 1).snapshotId())
            .planFiles();

    messages = getMessages(testAllEventQueue);
    clearQueue(messages, testAllEventQueue);
    Assert.assertEquals(4, messages.size());

    ObjectMapper objectMapper = new ObjectMapper();
    List<String> actualBodyNodesMessages = Lists.newArrayList();

    for (int i = 0; i < 4; i++) {
      actualBodyNodesMessages.add(objectMapper.readTree(messages.get(i).body()).toString());

    }

    Set<String> expectedBodyNodesMessages = Sets.newHashSet();
    expectedBodyNodesMessages.add("{\"table-name\":\"" + "glue." + namespace + "." + tableName + "\"," +
            "\"snapshot-id\":" + Iterables.get(snapshots, 0).snapshotId() + "," +
            "\"expression\":{\"type\":\"true\"},\"projection\":{\"type\":\"struct\"," +
            "\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"c1\"," +
            "\"required\":true,\"type\":\"string\",\"doc\":\"c1\"}]}}");
    expectedBodyNodesMessages.add("{\"table-name\":\"" + "glue." + namespace + "." + tableName + "\"," +
            "\"operation\":\"append\",\"snapshot-id\":" + Iterables.get(snapshots, 1).snapshotId() + "," +
            "\"sequence-number\":0,\"summary\":{\"added-data-files\":\"1\",\"added-records\":\"1\"," +
            "\"added-files-size\":\"10\",\"changed-partition-count\":\"1\",\"total-records\":\"2\"," +
            "\"total-files-size\":\"20\",\"total-data-files\":\"2\",\"total-delete-files\":\"0\"," +
            "\"total-position-deletes\":\"0\",\"total-equality-deletes\":\"0\"}}");
    expectedBodyNodesMessages.add("{\"table-name\":\"" + "glue." + namespace + "." + tableName + "\"," +
            "\"from-snapshot-id\":" + Iterables.get(snapshots, 0).snapshotId() + "," +
            "\"to-snapshot-id\":" + Iterables.get(snapshots, 1).snapshotId() + "," +
            "\"expression\":{\"type\":\"true\"},\"projection\":{\"type\":\"struct\"," +
            "\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"c1\"," +
            "\"required\":true,\"type\":\"string\",\"doc\":\"c1\"}]}}");
    expectedBodyNodesMessages.add("{\"table-name\":\"" + "glue." + namespace + "." + tableName + "\"," +
            "\"operation\":\"append\",\"snapshot-id\":" + Iterables.get(snapshots, 0).snapshotId() + "," +
            "\"sequence-number\":0,\"summary\":{\"added-data-files\":\"1\",\"added-records\":\"1\"," +
            "\"added-files-size\":\"10\",\"changed-partition-count\":\"1\",\"total-records\":\"1\"," +
            "\"total-files-size\":\"10\",\"total-data-files\":\"1\",\"total-delete-files\":\"0\"," +
            "\"total-position-deletes\":\"0\",\"total-equality-deletes\":\"0\"}}");

    Assert.assertEquals(4, expectedBodyNodesMessages.size());
    for (String message : actualBodyNodesMessages) {
      expectedBodyNodesMessages.remove(message);
    }

    Assert.assertEquals(0, expectedBodyNodesMessages.size());
  }

  public static void clearQueue(List<Message> messages, String sqsUrl) {
    for (Message m : messages) {
      DeleteMessageRequest req = DeleteMessageRequest.builder()
              .queueUrl(sqsUrl)
              .receiptHandle(m.receiptHandle())
              .build();
      sqs.deleteMessage(req);
    }
  }

  public static List<Message> getMessages(String sqsUrl) {
    ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
            .queueUrl(sqsUrl)
            .visibilityTimeout(100)
            .waitTimeSeconds(2)
            .maxNumberOfMessages(10)
            .build();

    List<Message> messages = Lists.newArrayList();
    messages.addAll(sqs.receiveMessage(receiveMessageRequest).messages());
    int prevCounter = -1;
    while (prevCounter != messages.size()) {
      prevCounter = messages.size();
      messages.addAll(sqs.receiveMessage(receiveMessageRequest).messages());
    }

    return messages;
  }
}
