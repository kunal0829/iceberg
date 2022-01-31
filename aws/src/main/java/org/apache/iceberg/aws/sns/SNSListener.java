/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.aws.sns;

import com.fasterxml.jackson.core.JsonGenerator;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;

import org.apache.iceberg.events.CreateSnapshotEvent;
import org.apache.iceberg.events.IncrementalScanEvent;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.util.EventParser;
import org.apache.iceberg.util.JsonUtil;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

public class SNSListener implements Listener {
  private String topicArn;
  // private AwsClientFactory awsClientFactory; // to be used later
  private SnsClient sns;

  public SNSListener(String inputARN, SnsClient snsInput) {
    sns = snsInput;
    topicArn = inputARN;
  }

  @Override
  public void notify(Object event) {
    StringWriter writer = new StringWriter();
    try {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      EventParser.toJson(event, generator);
      generator.flush();
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to write json"), e);
    }

    PublishRequest request = PublishRequest.builder().message(writer.toString()).topicArn(topicArn).build();
    sns.publish(request);

    sns.close();
  }
}
