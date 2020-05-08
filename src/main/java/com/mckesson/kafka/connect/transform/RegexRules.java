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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegexRules<R extends ConnectRecord<R>> extends AppyToTransformer<R> {

  static final Logger log = LoggerFactory.getLogger(RegexRules.class);
  private RegexRulesTextReplacer replacer;
  

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    this.replacer = new RegexRulesTextReplacer();
    this.replacer.configure(configs);
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
          transformed.add(value == null ? null : this.replacer.applyTransformations(value.toString()));
        }
        newValues.put(applyTo, transformed);
      }
    }
    return  buildNewRecord(record, newValues);
  }

  


  @Override
  public void close() {
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

}
