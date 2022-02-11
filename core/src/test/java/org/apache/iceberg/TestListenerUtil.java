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

package org.apache.iceberg;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestListenerUtil {
  @Test
  public void testDummyListener() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("listeners.ListenerName.test.client", "Test");
    properties.put("listeners.ListenerName.test.info", "Random");
    String name = "ListenerName";
    Listener listener = CatalogUtil.loadListener(TestListener.class.getName(), name, properties);
    Assertions.assertThat(listener).isInstanceOf(TestListener.class);
    Assert.assertEquals("Test", ((TestListener) listener).client);
    Assert.assertEquals("Random", ((TestListener) listener).info);
  }

  @Test
  public void testSingleRegEx() {
    Pattern pattern = Pattern.compile("^listeners[.](?<listenerName>.+)[.]impl$");
    Matcher match = pattern.matcher("listeners.prod.impl");
    Assert.assertTrue(match.matches());
    Assert.assertEquals("prod", match.group("listenerName"));
  }

  @Test
  public void testListCreation() {
    Map<String, String> properties = Maps.newHashMap();

    Configuration hadoopConf = new Configuration();
    String name = "custom";
    properties.put("listeners.listenerTestName.impl", TestListener.class.getName());
    properties.put("listeners.listenerTestName.test.info", "Information");
    properties.put("listeners.listenerTestName.test.client", "Client-Info");

    Catalog catalog = CatalogUtil.loadCatalog(TestListenerCatalog.class.getName(), name, properties, hadoopConf);

  }

  public static class TestListener<T> implements Listener<T> {
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
      this.info = properties.get("listeners." + listenerName + ".test.info");
      this.client = properties.get("listeners." + listenerName + ".test.client");
    }
  }

  public static class TestListenerCatalog extends BaseMetastoreCatalog {

    private String catalogName;
    private Map<String, String> flinkOptions;

    public TestListenerCatalog() {
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
      super.initialize(name, properties);
      this.catalogName = name;
      this.flinkOptions = properties;
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return null;
    }

    @Override
    public boolean dropTable(TableIdentifier identifier, boolean purge) {
      return false;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
    }
  }

}

