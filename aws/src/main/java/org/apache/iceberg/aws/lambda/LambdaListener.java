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

package org.apache.iceberg.aws.lambda;

import java.util.Map;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.util.EventParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

public class LambdaListener<T> implements Listener<T> {
  private static final Logger LOG = LoggerFactory.getLogger(LambdaListener.class);

  private LambdaClient lambda;
  private String functionName;

  public LambdaListener(Class<T> clazz) {
  }

  public LambdaListener(LambdaClient lambda, String functionName) {
    this.lambda = lambda;
  }

  @Override
  public void notify(Object event) {
    String msg = EventParser.toJson(event);
    InvokeRequest request = InvokeRequest.builder()
            .functionName(functionName)
            .payload(SdkBytes.fromUtf8String(("{\"message\":" + msg + "}")))
            .build();

    lambda.invoke(request);
  }

  @Override
  public void initialize(String listenerName, Map<String, String> properties) {
    AwsClientFactory factory = AwsClientFactories.from(properties);
    this.lambda = factory.lambda();

    if (properties.get(AwsProperties.LAMBDA_FUNCTION_NAME) == null) {
      throw new NullPointerException("Lambda function name cannot be null");
    }

    this.functionName = properties.get(AwsProperties.LAMBDA_FUNCTION_NAME);
  }
}

