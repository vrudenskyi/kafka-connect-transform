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

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Base class to transform structured message's field to value (structure that conatins message) 
 */
public class FieldToValueTransformation<R extends ConnectRecord<R>> implements Transformation<R> {

  static final Logger log = LoggerFactory.getLogger(FieldToValueTransformation.class);

  protected FieldToValueTransformationConfig config;
  protected String msgField;

  @Override
  public void configure(Map<String, ?> configs) {
    config = new FieldToValueTransformationConfig(configs);
    msgField = config.getString(FieldToValueTransformationConfig.FIELD_MESSAGE_CONFIG);
  }

  @Override
  public R apply(R record) {

    R newRecord = null;
    try {
      if (record.value() instanceof Struct) {
        newRecord = handleStructRecord(record);
      } else if (record.value() instanceof Map) {
        newRecord = handleMapRecord(record);
      } else if (record.value() instanceof String) {
        newRecord = handleStringRecord(record);
      } else {
        log.debug("Not supported record value, return as-is for class: {}", record.value().getClass());
      }
    } catch (Exception e) {
      log.debug("Error on transformation return as-is", e);
    }
    return newRecord == null ? record : newRecord;
  }

  private R handleStringRecord(R record) {
    String value = (String) record.value();

    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode valueObj = mapper.readTree(value);
      Object msgObject = valueObj.get(msgField).asText();

      R newRecord = record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          null,
          transformField(msgObject),
          record.timestamp());

      return newRecord;

    } catch (Exception e) {
      log.debug("Failed to process String as a JSON. Return unchanged.", e);

    }

    return record;
  }

  private R handleMapRecord(R record) {
    Map map = (Map) record.value();
    Object msgObject = map.get(msgField);
    if (msgObject == null) {
      log.debug("MapRecord.msgField ({}) is null for key={}, return null.", msgField, record.key());
      return null;
    }

    R newRecord = record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        null,
        transformField(msgObject),
        record.timestamp());

    return newRecord;
  }

  private R handleStructRecord(R record) {
    Struct struct = (Struct) record.value();
    Object msgBody = struct.get(msgField);
    if (msgBody == null) {
      log.debug("StructRecord.msgField ({}) is null for key={}, return null.", msgField, record.key());
      return null;
    }
    R newRecord = record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        null,
        transformField(msgBody),
        record.timestamp());

    return newRecord;
  }

  protected  Object transformField(Object msgObject) {
    return msgObject;
  }

  @Override
  public ConfigDef config() {
    return FieldToValueTransformationConfig.config();
  }

  @Override
  public void close() {
  }

}
