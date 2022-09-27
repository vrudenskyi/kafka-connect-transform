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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Implementation of regex rules based text 
**/
public class RegexRulesTextReplacer implements Configurable {
  
  static final Logger log = LoggerFactory.getLogger(RegexRulesTextReplacer.class);

  public static final String RULES_CONFIG = "rules";
  public static final String FAIL_ON_ERROR_CONFIG = "failOnError";
  public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(RULES_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, null, ConfigDef.Importance.HIGH, "List of aliases for rules")
        .define(FAIL_ON_ERROR_CONFIG, ConfigDef.Type.BOOLEAN, Boolean.FALSE, null, ConfigDef.Importance.HIGH, "Stop and throw exception if any rule failed");

  
  protected List<RegexMatchReplace> rules;
  boolean failOnError = false;


  @Override
  public void configure(Map<String, ?> configs) {
    // initialize transformations
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    List<String> replacerAliases = config.getList(RULES_CONFIG);
    this.rules = new LinkedList<RegexMatchReplace>();
    for (String alias : replacerAliases) {
      RegexMatchReplace replacer = new RegexMatchReplace();
      replacer.configure(config.originalsWithPrefix(RULES_CONFIG + "." + alias + "."));
      this.rules.add(replacer);
    }
  
  }

  public String applyTransformations(String value) {
    String result = value;
    for (RegexMatchReplace r : rules) {
      try {
        result = r.apply(result);
      } catch (Exception e) {
        log.warn("Failed to apply transformation: " + r, e);
        if (failOnError) {
          throw e;
        }
      }
    }
    return result;
  }

 
}