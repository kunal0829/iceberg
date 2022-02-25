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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.util.SerializableSupplier;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;

public class CloudWatchMetricsContext implements FileIOMetricsContext {
  public static final String NAMESPACE = "cloudwatch.namespace";

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
  }

  @Override
  public void initialize(Map<String, String> properties) {
    AwsClientFactory factory = AwsClientFactories.from(properties);

    if (cloudWatch == null) {
      this.cloudWatch = AwsClientFactories.from(properties)::cloudWatch;
    }
    this.namespace = properties.getOrDefault(NAMESPACE, "Iceberg/S3");
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
    private static CloudWatchClient cloudWatch;
    private String namespace;
    private String metric;
    private String unit;

    private static final int QUEUE_CAPACITY = 100;
    private static final int NUMBER_WORKERS = 2;
    private BlockingQueue<PutMetricDataRequest> metricsQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

    CloudWatchCounter(String metric, CloudWatchClient cloudWatch, String namespace, String unit) {
      Worker[] workers = new Worker[NUMBER_WORKERS];
      for (int i = 0; i < NUMBER_WORKERS; i++) {
        workers[i] = new Worker(metricsQueue);
        workers[i].start();
      }

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
      metricsQueue.add(dataRequest);
    }

    public static class Worker extends Thread {
      private BlockingQueue<PutMetricDataRequest> metricsQueue;

      Worker(BlockingQueue<PutMetricDataRequest> metricsQueue) {
        this.metricsQueue = metricsQueue;
      }

      @Override
      public void run() {
        try {
          while (true) {
            PutMetricDataRequest dataRequest = metricsQueue.take();
            if (dataRequest == null) {
              break;
            }
            cloudWatch.putMetricData(dataRequest);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
