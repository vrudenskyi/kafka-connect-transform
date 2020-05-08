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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class RemoveField<R extends ConnectRecord<R>> extends StructuredRecordTransform<R> {

  static final Logger log = LoggerFactory.getLogger(RemoveField.class);

  public static final String NAME_CONFIG = "name";
  public static final String NAMES_CONFIG = "names";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(NAME_CONFIG, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.HIGH, "Name of field.")
      //alias if multiply headers with the same value need to be added
      .define(NAMES_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(), null, ConfigDef.Importance.MEDIUM, "Names of field.");

  private List<String> allNames;

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);

    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    String name = config.getString(NAME_CONFIG);
    List<String> names = config.getList(NAMES_CONFIG);
    this.allNames = new ArrayList<>();
    if (name != null)
      this.allNames.add(name);
    if (names != null && names.size() > 0)
      this.allNames.addAll(names);
    if (this.allNames.size() == 0)
      throw new ConfigException("name(s) can't be empty");
  }

  protected R handleStringRecord(R record) {
    String value = (String) record.value();
    String newValue;

    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode valueObj = mapper.readTree(value);
      for (String pointer : allNames) {
        String parentPtr = pointer.substring(0, pointer.lastIndexOf('/'));
        String name = pointer.substring(pointer.lastIndexOf('/') + 1);
        if (StringUtils.isBlank(parentPtr)) {
          ((ObjectNode) valueObj).remove(name);
        } else {
          JsonNode parentNode = valueObj.at(parentPtr);
          if (!parentNode.isMissingNode() && parentNode.isObject()) {
            ((ObjectNode) valueObj.at(parentPtr)).remove(name);
          }
        }
      }
      newValue = mapper.writeValueAsString(valueObj);
    } catch (Exception e) {
      log.debug("Failed to process String as a JSON. Return unchanged.", e);
      return record;

    }

    return newRecord(record, newValue);
  }

  @Override
  protected R handlePrimitiveArrayRecord(R record) {
    if (byte.class.equals(record.value().getClass().getComponentType())) {
      return handleStringRecord(newRecord(record, new String((byte[]) record.value())));
    } else if (char.class.equals(record.value().getClass().getComponentType())) {
      return handleStringRecord(newRecord(record, new String((char[]) record.value())));
    }
    return record;
  }

  protected R handleMapRecord(R record) {
    Map value = (Map) record.value();
    Map updatedValue = new HashMap<>(value);
    for (String name : allNames) {
      updatedValue.remove(name);
    }
    return newRecord(record, updatedValue);
  }

  protected R handleStructRecord(R record) {
    Struct value = (Struct) record.value();
    final Struct updatedValue = new Struct(value.schema());

    for (Field field : value.schema().fields()) {
      if (!allNames.contains(field.name())) {
        final Object origFieldValue = value.get(field);
        updatedValue.put(field, origFieldValue);
      }
    }

    return newRecord(record, updatedValue);
  }

  protected R newRecord(R record, Object updatedValue) {
    return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

}
