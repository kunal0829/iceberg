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

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.cloudwatch.CloudWatchMetricsContext;
import org.apache.iceberg.metrics.MetricsContext;
import org.junit.Test;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;

public class TestCloudWatchMetricsContext extends GlueTestBase {
  private static final CloudWatchClient cw = clientFactory.cloudWatch();

  @Test
  public void testDataIncrement() {
    CloudWatchMetricsContext context = new CloudWatchMetricsContext(cw, "Check",
            CatalogProperties.DEFAULT_METRICS_MODE);
    MetricsContext.Counter count = context.counter("read.bytes", Long.class, MetricsContext.Unit.COUNT);
    count.increment(30);
  }
}
