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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ExtractField<R extends ConnectRecord<R>> extends StructuredRecordTransform<R> {

  static final Logger log = LoggerFactory.getLogger(ExtractField.class);
  
  private static ObjectMapper mapper = new ObjectMapper();
  static {
    mapper.configure(Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);  
  }
  


  public static final String NAME_CONFIG = "name";

  public static final String EXTRACT_TO_CONFIG = "extractTo";

  public static final String SKIP_EMPTY_CONFIG = "skipEmpty";
  public static final Boolean SKIP_EMPTY_DEFAULT = true;

  public static final List<String> EXTRACT_TO_DEFAULT = Arrays.asList("VALUE");
  public static final Validator EXTRACT_TO_VALIDATOR = new Validator() {
    
    

    private Pattern pattern = Pattern.compile("(?i)^key$|^value$|^topic$|^header\\..*$");

    @Override
    public void ensureValid(String name, Object value) {
      if (value == null)
        throw new ConfigException(name, value, "'extractTo' must be list of one of:  KEY, VALUE, TOPIC, HEADER.<header_name>");
      List<String> list = (List<String>) value;
      if (list.size() == 0)
        throw new ConfigException(name, value, "'extractTo' must contain a value of:  KEY, VALUE, TOPIC, HEADER.<header_name>");

      for (String str : list) {
        if (str == null || StringUtils.isBlank(str) || !pattern.matcher(str).matches()) {
          throw new ConfigException(name, value, "extractTo value must be one of:  KEY, VALUE, HEADER.<header_name>");
        }
      }
    }
  };

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(NAME_CONFIG, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.HIGH, "Name of field.")
      .define(EXTRACT_TO_CONFIG, ConfigDef.Type.LIST, EXTRACT_TO_DEFAULT, EXTRACT_TO_VALIDATOR, ConfigDef.Importance.HIGH, "Where to put extracted field. Supported values: KEY, VALUE, TOPIC, HEADER.<header_name>")
      .define(SKIP_EMPTY_CONFIG, ConfigDef.Type.BOOLEAN, SKIP_EMPTY_DEFAULT, null, ConfigDef.Importance.LOW, "If true will not extract if extraxted value is empty. Default: true");

  private String fieldName;
  private List<String> extractTo;
  private boolean skipEmpty;

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    this.fieldName = config.getString(NAME_CONFIG);
    this.extractTo = config.getList(EXTRACT_TO_CONFIG);
    this.skipEmpty = config.getBoolean(SKIP_EMPTY_CONFIG);

  }

  protected R handleStringRecord(R record) {
    String value = (String) record.value();
    JsonNode valueObj;
    try {
      valueObj = mapper.readTree(value);
    } catch (IOException e) {
      log.trace("String is not a json, failed to extract field", value, e);
      return record;
    }
    String extractedValue = valueObj.at(fieldName).asText();
    return newRecord(record, extractedValue);
  }

  @Override
  protected R handlePrimitiveArrayRecord(R record) {
    String value;
    if (byte.class.equals(record.value().getClass().getComponentType())) {
      value = new String((byte[]) record.value());
    } else if (char.class.equals(record.value().getClass().getComponentType())) {
      value = new String((char[]) record.value());
    } else {
      log.trace("record value is not an array or byte[] or char[]. Can't be handled as a string");
      return record;
    }

    ObjectMapper mapper = new ObjectMapper();
    JsonNode valueObj;
    try {
      valueObj = mapper.readTree(value);
    } catch (IOException e) {
      log.trace("String (from array) is not a json, failed to extract field", value, e);
      return record;
    }
    String extractedValue = valueObj.at(fieldName).asText();
    return newRecord(record, extractedValue);
  }

  protected R handleMapRecord(R record) {
    Map value = (Map) record.value();
    Object extractedValue = value.get(this.fieldName);
    return newRecord(record, extractedValue);
  }

  protected R handleStructRecord(R record) {
    Struct value = (Struct) record.value();
    Object extractedValue = value.get(this.fieldName);
    return newRecord(record, extractedValue);
  }

  protected R newRecord(R record, Object extractedValue) {

    if (skipEmpty && (extractedValue == null || StringUtils.isBlank(extractedValue.toString()))) {
      return record;
    }

    R newRec = record;
    for (String nameTo : extractTo) {
      newRec = writeValueTo(newRec, extractedValue, nameTo);
    }

    return newRec;
  }

  private R writeValueTo(R record, Object value, String writeTo) {

    if ("VALUE".equals(writeTo)) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), value, record.timestamp(), record.headers());
    } else if ("KEY".equals(writeTo)) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), value, record.valueSchema(), record.value(), record.timestamp(), record.headers());
    } else if ("TOPIC".equals(writeTo)) {
      if (value != null && StringUtils.isNotBlank(value.toString())) {
        return record.newRecord(value.toString(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp(), record.headers());
      }
    } else if (writeTo.startsWith("HEADER.") && writeTo.length() > 7) {
      if (value != null && StringUtils.isNotBlank(value.toString())) {
        record.headers().addString(writeTo.substring(7), value.toString());
      }
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
