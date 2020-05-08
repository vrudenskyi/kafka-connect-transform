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
package com.mckesson.kafka.connect.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import com.mckesson.kafka.connect.utils.ELUtils;

/**
 * 'Translate' one value with another using translation map
 * 
 * @param <R>
 */
public class MapTranslate<R extends ConnectRecord<R>> extends AppyToTransformer<R> {

  public static final String MAPS_CONFIG = "maps";
  public static final String FALLBACK_KEY_CONFIG = "fallbackKey";
  public static final ConfigDef ROOT_CONFIG_DEF = new ConfigDef()
      .define(MAPS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, null, ConfigDef.Importance.HIGH, "List of $aliases")
      .define(FALLBACK_KEY_CONFIG, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.MEDIUM, "Use if no matched maps. Supports EL.");
  

  public static final String KEY_CONFIG = "key";
  public static final String VALUES_CONFIG = "values";
  public static final ConfigDef CHILD_CONFIG_DEF = new ConfigDef()
      .define(KEY_CONFIG, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.HIGH, "Key, default value equeals to $alias")
      .define(VALUES_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, null, ConfigDef.Importance.HIGH, "List of items to search");

  private Map<String, String> map;
  private String fallbackKey;
  private boolean fallbackEL;
  

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);

    // initialize transformations
    map = new HashMap<>();
    SimpleConfig mapConfig = new SimpleConfig(ROOT_CONFIG_DEF, configs);
    List<String> maps = mapConfig.getList(MAPS_CONFIG);
    this.fallbackKey = mapConfig.getString(FALLBACK_KEY_CONFIG);
    this.fallbackEL = ELUtils.containsEL(this.fallbackKey);
    for (String alias : maps) {
      SimpleConfig childConfig;
      try {
        childConfig = new SimpleConfig(CHILD_CONFIG_DEF, mapConfig.originalsWithPrefix(MAPS_CONFIG + "." + alias + "."));
      } catch (ConfigException e) {
        throw new ConfigException("Wrong config for alias: " + alias, e);
      }
      String key = childConfig.getString(KEY_CONFIG);
      List<String> values = childConfig.getList(VALUES_CONFIG);
      if (StringUtils.isBlank(key)) {
        key = alias;
      }
      for (String v : values) {
        map.put(v, key);
      }

    }
  }

  @Override
  public R apply(R record) {

    Map<String, List<Object>> newValues = new HashMap<String, List<Object>>(applyToList.size());
    //read currentValues and transform
    for (String applyTo : applyToList) {
      List<Object> current = valuesFor(applyTo, record);
      if (current != null) {
        List<Object> transformed = new ArrayList<>(current.size());
        for (Object value : current) {
          if (map.containsKey(value)) {
            String resultValue = map.get(value);
            transformed.add(ELUtils.containsEL(resultValue) ? ELUtils.getExprValue(resultValue, record)  : resultValue);
          } else if (fallbackKey != null) {
            if (fallbackEL) {
              transformed.add(ELUtils.getExprValue(this.fallbackKey, record));
            } else {
              transformed.add(fallbackKey);
            }
          } else {
            transformed.add(value == null ? null : value.toString());
          }

        }
        newValues.put(applyTo, transformed);
      }
    }
    return buildNewRecord(record, newValues);
  }

  @Override
  public void close() {
  }

  @Override
  public ConfigDef config() {
    return ROOT_CONFIG_DEF;
  }

}
