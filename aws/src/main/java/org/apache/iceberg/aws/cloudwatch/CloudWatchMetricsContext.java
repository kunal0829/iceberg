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

package org.apache.iceberg.aws.cloudwatch;

import java.util.Map;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.util.SerializableSupplier;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;

public class CloudWatchMetricsContext implements FileIOMetricsContext {
  private SerializableSupplier<CloudWatchClient>  cloudWatch;
  private transient CloudWatchClient client;
  private String namespace;

  public CloudWatchMetricsContext() {
  }

  public CloudWatchMetricsContext(SerializableSupplier<CloudWatchClient> cloudWatch) {
    this((CloudWatchClient) cloudWatch, new String());
  }

  public CloudWatchMetricsContext(SerializableSupplier<CloudWatchClient> cloudWatch, String namespace) {
    this.cloudWatch =  cloudWatch;
    this.namespace = namespace;
  }

  private CloudWatchClient client() {
    if (client == null) {
      client = cloudWatch.get();
    }
    return client;
  }

  public CloudWatchMetricsContext(CloudWatchClient cloudWatch, String namespace) {
    this.client = cloudWatch;
    this.namespace = namespace;

    MetricDatum readBytes = MetricDatum.builder().value(0.0).unit("Bytes").metricName("ReadBytes").build();
    MetricDatum readOperations = MetricDatum.builder().value(0.0).unit("Count").metricName("ReadOperations").build();
    MetricDatum writeBytes = MetricDatum.builder().value(0.0).unit("Bytes").metricName("WriteBytes").build();
    MetricDatum writeOperations = MetricDatum.builder().value(0.0).unit("Count").metricName("WriteOperations").build();

    PutMetricDataRequest readBytesRequest =
            PutMetricDataRequest.builder().namespace(namespace).metricData(readBytes).build();
    PutMetricDataRequest readOperationsRequest =
            PutMetricDataRequest.builder().namespace(namespace).metricData(readOperations).build();
    PutMetricDataRequest writeBytesRequest =
            PutMetricDataRequest.builder().namespace(namespace).metricData(writeBytes).build();
    PutMetricDataRequest writeOperationsRequest =
            PutMetricDataRequest.builder().namespace(namespace).metricData(writeOperations).build();

    this.client.putMetricData(readBytesRequest);
    this.client.putMetricData(readOperationsRequest);
    this.client.putMetricData(writeBytesRequest);
    this.client.putMetricData(writeOperationsRequest);
  }

  @Override
  public void initialize(Map<String, String> properties) {
    AwsClientFactory factory = AwsClientFactories.from(properties);

    if (cloudWatch == null) {
      this.cloudWatch = AwsClientFactories.from(properties)::cloudWatch;
    }
    this.namespace = "IcebergS3";

    MetricDatum readBytes = MetricDatum.builder().value(0.0).unit("Bytes").metricName("ReadBytes").build();
    MetricDatum readOperations = MetricDatum.builder().value(0.0).unit("Count").metricName("ReadOperations").build();
    MetricDatum writeBytes = MetricDatum.builder().value(0.0).unit("Bytes").metricName("WriteBytes").build();
    MetricDatum writeOperations = MetricDatum.builder().value(0.0).unit("Count").metricName("WriteOperations").build();

    PutMetricDataRequest readBytesRequest =
            PutMetricDataRequest.builder().namespace(namespace).metricData(readBytes).build();
    PutMetricDataRequest readOperationsRequest =
            PutMetricDataRequest.builder().namespace(namespace).metricData(readOperations).build();
    PutMetricDataRequest writeBytesRequest =
            PutMetricDataRequest.builder().namespace(namespace).metricData(writeBytes).build();
    PutMetricDataRequest writeOperationsRequest =
            PutMetricDataRequest.builder().namespace(namespace).metricData(writeOperations).build();

    client().putMetricData(readBytesRequest);
    client().putMetricData(readOperationsRequest);
    client().putMetricData(writeBytesRequest);
    client().putMetricData(writeOperationsRequest);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Number> Counter<T> counter(String name, Class<T> type, Unit unit) {
    switch (name) {
      case READ_BYTES:
        ValidationException.check(type == Long.class, "'%s' requires Long type", READ_BYTES);
        return new CloudWatchCounter("ReadBytes", client(), this.namespace, "Bytes");
      case READ_OPERATIONS:
        ValidationException.check(type == Integer.class, "'%s' requires Integer type", READ_OPERATIONS);
        return new CloudWatchCounter("ReadOperations", client(), this.namespace, "Count");
      case WRITE_BYTES:
        ValidationException.check(type == Long.class, "'%s' requires Long type", WRITE_BYTES);
        return new CloudWatchCounter("WriteBytes", client(), this.namespace, "Bytes");
      case WRITE_OPERATIONS:
        ValidationException.check(type == Integer.class, "'%s' requires Integer type", WRITE_OPERATIONS);
        return new CloudWatchCounter("WriteOperations", client(), this.namespace, "Count");
      default:
        throw new IllegalArgumentException(String.format("Unsupported counter: '%s'", name));
    }
  }

  private static class CloudWatchCounter implements Counter {
    private CloudWatchClient cloudWatch;
    private String namespace;
    private String metric;
    private String unit;

    CloudWatchCounter(String metric, CloudWatchClient cloudWatch, String namespace, String unit) {
      this.cloudWatch = cloudWatch;
      this.namespace = namespace;
      this.metric = metric;
      this.unit = unit;
    }

    @Override
    public void increment() {
      increment(1);
    }

    @Override
    public void increment(Number amount) {
      MetricDatum metricData = MetricDatum.builder().value(amount.doubleValue()).unit(unit).metricName(metric).build();
      PutMetricDataRequest dataRequest =
              PutMetricDataRequest.builder().namespace(namespace).metricData(metricData).build();
      cloudWatch.putMetricData(dataRequest);
    }
  }
}
