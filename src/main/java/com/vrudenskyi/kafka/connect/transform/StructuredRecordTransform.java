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

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vrudensk.kafka.connect.utils.ELUtils;

public abstract class StructuredRecordTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  static final Logger log = LoggerFactory.getLogger(StructuredRecordTransform.class);

  public static final String CONDITION_CONFIG = "condition";
  

  private IfRegex ifRegex;
  

  //TODO: implement multiply conditions
  public static final ConfigDef CONFIG_DEF = new ConfigDef();

  @Override
  public void configure(Map<String, ?> configs) {

    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);

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

    R newRecord = null;
    try {
      if (record.value() instanceof Struct) {
        newRecord = handleStructRecord(record);
      } else if (record.value() instanceof Map) {
        newRecord = handleMapRecord(record);
      } else if (record.value() instanceof String) {
        newRecord = handleStringRecord(record);
      } else if (record.value() != null && record.value().getClass().isArray() && record.value().getClass().getComponentType().isPrimitive()) {
        newRecord = handlePrimitiveArrayRecord(record);
      } else {
        log.trace("Not supported record value, return as-is for class: {}", record.value().getClass());
      }
    } catch (Exception e) {
      log.trace("Error on transformation return as-is", e);
    }
    return newRecord == null ? record : newRecord;
  }

  protected abstract R handleStructRecord(R record);

  protected abstract R handleMapRecord(R record);

  protected abstract R handleStringRecord(R record);

  protected abstract R handlePrimitiveArrayRecord(R record);

}
