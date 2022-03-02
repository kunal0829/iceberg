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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.SerializableSupplier;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.cloudwatchlogs.emf.config.Configuration;
import software.amazon.cloudwatchlogs.emf.config.EnvironmentConfigurationProvider;
import software.amazon.cloudwatchlogs.emf.environment.DefaultEnvironment;
import software.amazon.cloudwatchlogs.emf.logger.MetricsLogger;

public class CloudWatchMetricsContext implements FileIOMetricsContext {
  private SerializableSupplier<CloudWatchClient>  cloudWatch;
  private transient CloudWatchClient client;
  private SerializableSupplier<MetricsLogger>  metricsLogger;
  private transient MetricsLogger logger;
  private String namespace;
  private String mode;

  public CloudWatchMetricsContext() {
  }

  public CloudWatchMetricsContext(SerializableSupplier<CloudWatchClient> cloudWatch) {
    this((CloudWatchClient) cloudWatch,
            CatalogProperties.DEFAULT_CLOUDWATCH_NAMESPACE,
            CatalogProperties.DEFAULT_METRICS_MODE);
  }

  public CloudWatchMetricsContext(SerializableSupplier<CloudWatchClient> cloudWatch,
                                  MetricsLogger logger, String namespace, String mode) {
    this.cloudWatch =  cloudWatch;
    this.namespace = namespace;
    this.mode = mode;
    this.logger = logger;
  }

  private CloudWatchClient client() {
    if (client == null) {
      client = cloudWatch.get();
    }
    return client;
  }

  private MetricsLogger metricsLogger() {
    if (logger == null) {
      logger = metricsLogger.get();
    }
    return logger;
  }

  public CloudWatchMetricsContext(CloudWatchClient cloudWatch, String namespace, String mode) {
    this.client = cloudWatch;
    this.namespace = namespace;
    this.mode = mode;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    AwsClientFactory factory = AwsClientFactories.from(properties);

    if (cloudWatch == null) {
      this.cloudWatch = AwsClientFactories.from(properties)::cloudWatch;
    }

    this.namespace = properties.getOrDefault(CatalogProperties.CLOUDWATCH_NAMESPACE,
            CatalogProperties.DEFAULT_CLOUDWATCH_NAMESPACE);
    this.mode = properties.getOrDefault(CatalogProperties.CLOUDWATCH_MODE, "normal");

    Configuration config = EnvironmentConfigurationProvider.getConfig();
    DefaultEnvironment environment = new DefaultEnvironment(EnvironmentConfigurationProvider.getConfig());

    this.logger = new MetricsLogger(environment);
    this.logger.setNamespace(this.namespace);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Number> Counter<T> counter(String name, Class<T> type, Unit unit) {
    switch (name) {
      case READ_BYTES:
        ValidationException.check(type == Long.class, "'%s' requires Long type", READ_BYTES);
        if (this.mode.equals(CatalogProperties.CLOUDWATCH_EMBEDDED)) {
          return new CloudWatchEmbeddedMetricsCounter("ReadBytes", metricsLogger(), this.namespace, "Bytes");
        } else {
          return new CloudWatchCounter("ReadBytes", client(), this.namespace, "Bytes");
        }
      case READ_OPERATIONS:
        ValidationException.check(type == Integer.class, "'%s' requires Integer type", READ_OPERATIONS);
        if (this.mode.equals(CatalogProperties.CLOUDWATCH_EMBEDDED)) {
          return new CloudWatchEmbeddedMetricsCounter("ReadOperations", metricsLogger(), this.namespace, "Count");
        } else {
          return new CloudWatchCounter("ReadOperations", client(), this.namespace, "Count");
        }
      case WRITE_BYTES:
        ValidationException.check(type == Long.class, "'%s' requires Long type", WRITE_BYTES);
        if (this.mode.equals(CatalogProperties.CLOUDWATCH_EMBEDDED)) {
          return new CloudWatchEmbeddedMetricsCounter("WriteBytes", metricsLogger(), this.namespace, "Bytes");
        } else {
          return new CloudWatchCounter("WriteBytes", client(), this.namespace, "Bytes");
        }

      case WRITE_OPERATIONS:
        ValidationException.check(type == Integer.class, "'%s' requires Integer type", WRITE_OPERATIONS);
        if (this.mode.equals(CatalogProperties.CLOUDWATCH_EMBEDDED)) {
          return new CloudWatchEmbeddedMetricsCounter("WriteOperations", metricsLogger(), this.namespace, "Count");
        } else {
          return new CloudWatchCounter("WriteOperations", client(), this.namespace, "Count");
        }
      default:
        throw new IllegalArgumentException(String.format("Unsupported counter: '%s'", name));
    }
  }

  private static class CloudWatchCounter implements Counter {
    private static ExecutorService executorService;
    private static CloudWatchClient cloudWatch;
    private String namespace;
    private String metric;
    private String unit;
    private static final int NUMBER_THREADS = 10;

    CloudWatchCounter(String metric, CloudWatchClient cloudWatch, String namespace, String unit) {
      if (executorService == null) {
        synchronized (CloudWatchCounter.class) {
          if (executorService == null) {
            executorService = MoreExecutors.getExitingExecutorService(
                    (ThreadPoolExecutor) Executors.newFixedThreadPool(
                            NUMBER_THREADS,
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("iceberg-cloudwatch-metric-%d")
                                    .build()));
          }
        }
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
      MetricDatum metricData = MetricDatum
              .builder()
              .value(amount.doubleValue())
              .unit(unit)
              .metricName(metric)
              .build();

      executorService.execute(new Runnable() {
        public void run() {
          PutMetricDataRequest dataRequest = PutMetricDataRequest
                  .builder()
                  .namespace(namespace)
                  .metricData(metricData)
                  .build();
          cloudWatch.putMetricData(dataRequest);
        }
      });
    }
  }

  private static class CloudWatchEmbeddedMetricsCounter implements Counter {
    private static MetricsLogger metricsLogger;
    private String namespace;
    private String metric;
    private String unit;

    CloudWatchEmbeddedMetricsCounter(String metric, MetricsLogger metricsLogger, String namespace, String unit) {
      this.metricsLogger = metricsLogger;
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

    }
  }
}
