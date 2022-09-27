/**
 * Copyright  Vitalii Rudenskyi (vrudenskyi@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.vrudenskyi.kafka.connect.transform;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import com.vrudensk.kafka.connect.utils.ELUtils;

public class AddHeader<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String NAME_CONFIG = "name";
  public static final String NAMES_CONFIG = "names";
  public static final String VALUE_CONFIG = "value";
  public static final String REPLACE_CONFIG = "replace";
  public static final String SCHEMA_CONFIG = "schema";
  public static final String SCHEMA_DEFAULT = "STRING";
  public static final String CONDITION_CONFIG = "condition";
  

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(NAME_CONFIG, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.HIGH, "Name of header.")
      .define(NAMES_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(), null, ConfigDef.Importance.MEDIUM, "Names of header.")//alias if multiply headers with the same value need to be added
      .define(REPLACE_CONFIG, ConfigDef.Type.BOOLEAN, Boolean.FALSE, null, ConfigDef.Importance.MEDIUM, "If true replace existing value, if false add. Default: false")
      .define(VALUE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, null, ConfigDef.Importance.HIGH, "Value of header. Supports copy from key, value or another header")
      .define(SCHEMA_CONFIG, ConfigDef.Type.STRING, SCHEMA_DEFAULT, null, ConfigDef.Importance.LOW, "Default is string or copied from original (key,value, header)");

  private List<String> allNames;
  private String value;
  private Schema schema;
  private IfRegex ifRegex;
  
  private boolean replace;

  private boolean hasEL = false;

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    String name = config.getString(NAME_CONFIG);
    List<String> names = config.getList(NAMES_CONFIG);
    this.allNames = new ArrayList<>();
    if (name != null)
      this.allNames.add(name);
    if (names != null && names.size() > 0)
      this.allNames.addAll(names);
    if (this.allNames.size() == 0) {
      throw new ConfigException("name(s) can't be empty");
    }
    this.value = config.getString(VALUE_CONFIG);
    this.schema = new SchemaBuilder(Type.valueOf(config.getString(SCHEMA_CONFIG))).build();
    this.hasEL = ELUtils.containsEL(this.value);
    this.replace = config.getBoolean(REPLACE_CONFIG);

    this.ifRegex = new IfRegex();
    Map<String, Object> conditionConfig = config.originalsWithPrefix(CONDITION_CONFIG + ".");
    if (conditionConfig.size() > 0) {
      this.ifRegex.configure(conditionConfig);
      
    }

  }

  @Override
  public R apply(R record) {

    if (ifRegex.ifConfigured() && !ifRegex.checkIf(ELUtils.getExprValue(ifRegex.getIfData(), record))) {
      return record;
    }

    final String exprValue = hasEL ? ELUtils.getExprValue(this.value, record) : this.value;
    if (StringUtils.isNotBlank(exprValue)) {
      this.allNames.forEach(name -> {
        if (replace) {
          record.headers().remove(name);  
        }
        record.headers().add(name, exprValue, schema);
      });
    }

    return record;
  }

  @Override
  public ConfigDef config() {

    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

}
