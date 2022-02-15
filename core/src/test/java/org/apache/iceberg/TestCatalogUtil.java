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
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.events.Listener;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestCatalogUtil {

  @Test
  public void loadCustomCatalog() {
    Map<String, String> options = Maps.newHashMap();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";
    Catalog catalog = CatalogUtil.loadCatalog(TestCatalog.class.getName(), name, options, hadoopConf);
    Assertions.assertThat(catalog).isInstanceOf(TestCatalog.class);
    Assert.assertEquals(name, ((TestCatalog) catalog).catalogName);
    Assert.assertEquals(options, ((TestCatalog) catalog).flinkOptions);
  }

  @Test
  public void loadCustomCatalog_withHadoopConfig() {
    Map<String, String> options = Maps.newHashMap();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("key", "val");
    String name = "custom";
    Catalog catalog = CatalogUtil.loadCatalog(TestCatalogConfigurable.class.getName(), name, options, hadoopConf);
    Assertions.assertThat(catalog).isInstanceOf(TestCatalogConfigurable.class);
    Assert.assertEquals(name, ((TestCatalogConfigurable) catalog).catalogName);
    Assert.assertEquals(options, ((TestCatalogConfigurable) catalog).flinkOptions);
    Assert.assertEquals(hadoopConf, ((TestCatalogConfigurable) catalog).configuration);
  }

  @Test
  public void loadCustomCatalog_NoArgConstructorNotFound() {
    Map<String, String> options = Maps.newHashMap();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";
    AssertHelpers.assertThrows("must have no-arg constructor",
        IllegalArgumentException.class,
        "NoSuchMethodException: org.apache.iceberg.TestCatalogUtil$TestCatalogBadConstructor.<init>()",
        () -> CatalogUtil.loadCatalog(TestCatalogBadConstructor.class.getName(), name, options, hadoopConf));
  }

  @Test
  public void loadCustomCatalog_NotImplementCatalog() {
    Map<String, String> options = Maps.newHashMap();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";

    AssertHelpers.assertThrows("must implement catalog",
        IllegalArgumentException.class,
        "does not implement Catalog",
        () -> CatalogUtil.loadCatalog(TestCatalogNoInterface.class.getName(), name, options, hadoopConf));
  }

  @Test
  public void loadCustomCatalog_ConstructorErrorCatalog() {
    Map<String, String> options = Maps.newHashMap();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";

    String impl = TestCatalogErrorConstructor.class.getName();
    AssertHelpers.assertThrows("must be able to initialize catalog",
        IllegalArgumentException.class,
        "NoClassDefFoundError: Error while initializing class",
        () -> CatalogUtil.loadCatalog(impl, name, options, hadoopConf));
  }

  @Test
  public void loadCustomCatalog_BadCatalogNameCatalog() {
    Map<String, String> options = Maps.newHashMap();
    options.put("key", "val");
    Configuration hadoopConf = new Configuration();
    String name = "custom";
    String impl = "CatalogDoesNotExist";
    AssertHelpers.assertThrows("catalog must exist",
        IllegalArgumentException.class,
        "java.lang.ClassNotFoundException: CatalogDoesNotExist",
        () -> CatalogUtil.loadCatalog(impl, name, options, hadoopConf));
  }

  @Test
  public void loadCustomFileIO_noArg() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key", "val");
    FileIO fileIO = CatalogUtil.loadFileIO(TestFileIONoArg.class.getName(), properties, null);
    Assertions.assertThat(fileIO).isInstanceOf(TestFileIONoArg.class);
    Assert.assertEquals(properties, ((TestFileIONoArg) fileIO).map);
  }

  @Test
  public void loadCustomFileIO_hadoopConfigConstructor() {
    Configuration configuration = new Configuration();
    configuration.set("key", "val");
    FileIO fileIO = CatalogUtil.loadFileIO(HadoopFileIO.class.getName(), Maps.newHashMap(), configuration);
    Assertions.assertThat(fileIO).isInstanceOf(HadoopFileIO.class);
    Assert.assertEquals("val", ((HadoopFileIO) fileIO).conf().get("key"));
  }

  @Test
  public void loadCustomFileIO_configurable() {
    Configuration configuration = new Configuration();
    configuration.set("key", "val");
    FileIO fileIO = CatalogUtil.loadFileIO(TestFileIOConfigurable.class.getName(), Maps.newHashMap(), configuration);
    Assertions.assertThat(fileIO).isInstanceOf(TestFileIOConfigurable.class);
    Assert.assertEquals(configuration, ((TestFileIOConfigurable) fileIO).configuration);
  }

  @Test
  public void loadCustomFileIO_badArg() {
    AssertHelpers.assertThrows("cannot find constructor",
        IllegalArgumentException.class,
        "missing no-arg constructor",
        () -> CatalogUtil.loadFileIO(TestFileIOBadArg.class.getName(), Maps.newHashMap(), null));
  }

  @Test
  public void loadCustomFileIO_badClass() {
    AssertHelpers.assertThrows("cannot cast",
        IllegalArgumentException.class,
        "does not implement FileIO",
        () -> CatalogUtil.loadFileIO(TestFileIONotImpl.class.getName(), Maps.newHashMap(), null));
  }

  @Test
  public void buildCustomCatalog_withTypeSet() {
    Map<String, String> options = Maps.newHashMap();
    options.put(CatalogProperties.CATALOG_IMPL, "CustomCatalog");
    options.put(CatalogUtil.ICEBERG_CATALOG_TYPE, "hive");
    Configuration hadoopConf = new Configuration();
    String name = "custom";

    AssertHelpers.assertThrows("Should complain about both configs being set", IllegalArgumentException.class,
        "both type and catalog-impl are set", () -> CatalogUtil.buildIcebergCatalog(name, options, hadoopConf));
  }

  @Test
  public void testSingleRegEx() {
    Pattern pattern = Pattern.compile("^listeners[.](?<name>.+)[.]impl$");
    Matcher matchTrue = pattern.matcher("listeners.prod.impl");
    Matcher matchFalse = pattern.matcher("listeners.prod.iampl");
    Assert.assertTrue(matchTrue.matches());
    Assert.assertFalse(matchFalse.matches());
    Assert.assertEquals("prod", matchTrue.group("name"));
  }

  @Test
  public void testLoadListener() {
    Map<String, String> properties = Maps.newHashMap();
    String listenerName = "ListenerName";
    properties.put("impl", TestListener.class.getName());
    properties.put("test.client", "Client-Info");
    properties.put("test.info", "Information");
    Pattern listenerMatch = Pattern.compile("^listeners[.](?<name>[^[.]]+)[.](?<config>.+)$");
    String listenerNameField = "name";
    String listenerConfigField = "config";

    Map<String, Map<String, String>> listenerProperties = Maps.newHashMap();
    for (String key : properties.keySet()) {
      Matcher match = listenerMatch.matcher(key);
      if (match.matches()) {
        if (listenerProperties.containsKey(match.group(listenerNameField))) {
          listenerProperties.get(match.group(listenerNameField))
                          .put(match.group(listenerConfigField), properties.get(key));
        } else {
          Map<String, String> toadd = Maps.newHashMap();
          toadd.put(match.group(listenerConfigField), properties.get(key));
          listenerProperties.put(match.group(listenerNameField), toadd);
        }
      }
    }

    Listener listener = CatalogUtil.loadListener(TestListener.class.getName(), listenerName, properties);
    Assertions.assertThat(listener).isInstanceOf(TestListener.class);
    Assert.assertEquals("Client-Info", ((TestListener) listener).client);
    Assert.assertEquals("Information", ((TestListener) listener).info);
  }

  @Test
  public void loadBadListenerClass() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key", "val");
    String name = "custom";
    String impl = "ListenerDoesNotExist";
    AssertHelpers.assertThrows("Listener must exist",
            IllegalArgumentException.class,
            "Cannot initialize Listener",
            () -> CatalogUtil.loadListener(impl, name, properties));
  }

  @Test
  public void loadBadListenerConstructor() {
    String name = "custom";
    AssertHelpers.assertThrows("cannot find constructor",
            IllegalArgumentException.class,
            "missing no-arg constructor",
            () -> CatalogUtil.loadListener(TestListenerBadConstructor.class.getName(), name, Maps.newHashMap()));
  }

  public static class TestCatalog extends BaseMetastoreCatalog {

    private String catalogName;
    private Map<String, String> flinkOptions;

    public TestCatalog() {
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

  public static class TestCatalogConfigurable extends BaseMetastoreCatalog implements Configurable {

    private String catalogName;
    private Map<String, String> flinkOptions;
    private Configuration configuration;

    public TestCatalogConfigurable() {
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
      this.catalogName = name;
      this.flinkOptions = properties;
    }

    @Override
    public void setConf(Configuration conf) {
      this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
      return configuration;
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

  public static class TestCatalogBadConstructor extends BaseMetastoreCatalog {

    public TestCatalogBadConstructor(String arg) {
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

    @Override
    public void initialize(String name, Map<String, String> properties) {
    }
  }

  public static class TestCatalogNoInterface {
    public TestCatalogNoInterface() {
    }
  }

  public static class TestFileIOConfigurable implements FileIO, Configurable {

    private Configuration configuration;

    public TestFileIOConfigurable() {
    }

    @Override
    public void setConf(Configuration conf) {
      this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
      return configuration;
    }

    @Override
    public InputFile newInputFile(String path) {
      return null;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return null;
    }

    @Override
    public void deleteFile(String path) {

    }

    public Configuration getConfiguration() {
      return configuration;
    }
  }

  public static class TestFileIONoArg implements FileIO {

    private Map<String, String> map;

    public TestFileIONoArg() {
    }

    @Override
    public InputFile newInputFile(String path) {
      return null;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return null;
    }

    @Override
    public void deleteFile(String path) {

    }

    public Map<String, String> getMap() {
      return map;
    }

    @Override
    public void initialize(Map<String, String> properties) {
      map = properties;
    }
  }

  public static class TestFileIOBadArg implements FileIO {

    private final String arg;

    public TestFileIOBadArg(String arg) {
      this.arg = arg;
    }

    @Override
    public InputFile newInputFile(String path) {
      return null;
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return null;
    }

    @Override
    public void deleteFile(String path) {

    }

    public String getArg() {
      return arg;
    }
  }

  public static class TestFileIONotImpl {
    public TestFileIONotImpl() {
    }
  }

  public static class TestListener<T> implements Listener<T> {
    private String client;
    private String info;
    private String name;

    public TestListener() {
    }

    @Override
    public void notify(Object event) {
    }

    @Override
    public void initialize(String listenerName, Map<String, String> properties) {
      this.name = listenerName;
      this.info = properties.get("test.info");
      this.client = properties.get("test.client");
    }
  }

  public static class TestListenerBadConstructor<T> implements Listener<T> {
    private String arg;

    public TestListenerBadConstructor(String arg) {
      this.arg = arg;
    }

    @Override
    public void notify(Object event) {
    }

    @Override
    public void initialize(String listenerName, Map<String, String> properties) {
      this.arg = listenerName;
    }
  }
}
