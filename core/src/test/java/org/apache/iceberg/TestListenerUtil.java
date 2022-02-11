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

package org.apache.iceberg;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestListenerUtil {
  @Test
  public void testDummyListener() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("client", "Test");
    properties.put("info", "Random");
    String name = "ListenerName";
    Listener listener = CatalogUtil.loadListener(TestListener.class.getName(), name, properties);
    Assertions.assertThat(listener).isInstanceOf(TestListener.class);
    Assert.assertEquals("Test", ((TestListener) listener).client);
    Assert.assertEquals("Random", ((TestListener) listener).info);
  }

  public class TestListener<T> implements Listener<T> {
    private String client;
    private String info;
    private String name;

    public TestListener() {
    }

    @Override
    public void notify(Object event) {
      System.out.println("Notify");
    }

    @Override
    public void initialize(String listenerName, Map<String, String> properties) {
      this.name = listenerName;
      this.client = properties.get("client");
      this.info = properties.get("info");
    }
  }
}
