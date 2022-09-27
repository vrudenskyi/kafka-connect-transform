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

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replaces recor's topic based on regex rules 
 * 
 * @param <R>
 */
public class RegexRouter<R extends ConnectRecord<R>> extends AppyToTransformer<R> {
  public static final String RULES_CONFIG = "rules";
  public static final String FALLBACK_TOPIC_CONFIG = "fallbackTopic";

  static final Logger log = LoggerFactory.getLogger(RegexRouter.class);

  //private String dataFieldExpression;
  private String fallbackTopic;
  private static final String RECORD_DROP = "";

  private LinkedHashMap<String, RegexMatchReplace> matchers;

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(RULES_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, null, ConfigDef.Importance.HIGH,
          "List of aliases for matchers")
      .define(FALLBACK_TOPIC_CONFIG, ConfigDef.Type.STRING, null, null,
          ConfigDef.Importance.MEDIUM, "Topic name to use if no matching rules");

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    // initialize matchers
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    List<String> ruleAliases = config.getList(RULES_CONFIG);
    matchers = new LinkedHashMap<String, RegexMatchReplace>(ruleAliases.size());
    for (String alias : ruleAliases) {
      RegexMatchReplace matcher = new RegexMatchReplace();
      matcher.configure(config.originalsWithPrefix(RULES_CONFIG + "." + alias + "."));
      matchers.put(alias, matcher);
    }

    this.fallbackTopic = config.getString(FALLBACK_TOPIC_CONFIG);

  }

  @Override
  public R apply(R record) {

    String topic = null;
    //read currentValues and transform
    for (String applyTo : applyToList) {
      List<Object> applyToValues = valuesFor(applyTo, record);
      for (Object applyToValue : applyToValues) {
        topic = topicForValue(applyToValue);
        if (topic != null) {
          break;
        }
      }
      if (topic != null) {
        break;
      }
    }

    if (topic == null && fallbackTopic != null) {
      log.trace("No matching rules, use fallback topic: '{}'", fallbackTopic);
      topic = fallbackTopic;
    }

    //if enpty string drop message
    if (RECORD_DROP.equals(topic)) {
      log.trace("Topic is empty string. Will drop record.", fallbackTopic);
      return null;
    }

    if (topic != null) {
      return record.newRecord(topic, record.kafkaPartition(), record.keySchema(), record.key(),
          record.valueSchema(), record.value(), record.timestamp());
    }

    // no matching rules + no fallbackTopic => return as-is
    log.trace("No matching rules, no fallback topic. Return as-is");
    return record;

  }

  /**
   * return topicname based on configure rules 
   * 
   * @param value
   * @return
   *    name of topic  
   *    null - if no value
   *    empty string - indicates record must be dropped
   */
  private String topicForValue(Object value) {
    if (value == null) {
      log.trace("topicForValue: input value is null, calculated topic is null");
      return null;
    }

    String data = value.toString();
    // process rules for data
    for (RegexMatchReplace matcher : matchers.values()) {
      if (matcher.checkIf(data)) {
        //drop messages for null replacement
        if (StringUtils.isBlank(matcher.getReplacement())) {
          log.trace("Topic replacement is null. Drop message.");
          return RECORD_DROP;
        }
        return matcher.getReplacement();
      }
    }

    log.trace("topicForValue: no matching rules, calculated topic is null");
    return null;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

}
