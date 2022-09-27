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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter based on regex rules 
 * 
 * @author ew4ahmz
 *
 * @param <R>
 */
public class RegexFilter<R extends ConnectRecord<R>> extends AppyToTransformer<R> {
  public static final String CONDITIONS_CONFIG = "conditions";

  //TODO: implement dropAny, dropAll, keepAny, keepAll 
  public static final String FILTER_TYPE_CONFIG = "filterType";
  public static final String FILTER_TYPE_DEFAULT = "drop";

  static final Logger log = LoggerFactory.getLogger(RegexFilter.class);

  private LinkedHashMap<String, IfRegex> conditions;
  private String filterType;

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(CONDITIONS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, null, ConfigDef.Importance.HIGH, "List of aliases for drop conditions")
      .define(FILTER_TYPE_CONFIG, ConfigDef.Type.STRING, FILTER_TYPE_DEFAULT, ValidString.in("drop", "keep"), ConfigDef.Importance.HIGH, "The way filter works: 'drop' if match or 'keep' if match. Values: drop, keep. Default: drop");

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    // initialize matchers
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    List<String> cAliases = config.getList(CONDITIONS_CONFIG);
    conditions = new LinkedHashMap<String, IfRegex>(cAliases.size());
    for (String alias : cAliases) {
      IfRegex cond = new IfRegex();
      cond.configure(config.originalsWithPrefix(CONDITIONS_CONFIG + "." + alias + "."));
      if (!cond.ifConfigured()) {
        throw new ConnectException("Condition not configured for alias:" + alias);
      }
      conditions.put(alias, cond);
    }
    filterType = config.getString(FILTER_TYPE_CONFIG).toLowerCase();

  }

  @Override
  public R apply(R record) {

    boolean matches = false;

    for (String applyTo : applyToList) {
      List<Object> applyToValues = valuesFor(applyTo, record);
      for (Object applyToValue : applyToValues) {
        for (IfRegex rule : conditions.values()) {
          matches = applyToValue != null && rule.checkIf(applyToValue.toString());
          if (matches) {
            break;
          }
        }
        if (matches) {
          break;
        }
      }
      if (matches) {
        break;
      }
    }

    if ((matches && "drop".equals(filterType))
        || (!matches && "keep".equals(filterType))) {
      return null;
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
