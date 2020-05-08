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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mckesson.kafka.connect.utils.ELUtils;

public abstract class AppyToTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

  static final Logger log = LoggerFactory.getLogger(AppyToTransformer.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String APPLY_TO_CONFIG = "applyTo";
  public static final List<String> APPLY_TO_DEFAULT = Arrays.asList("VALUE");
  public static final String APPLY_TO_VALUE_CONFIG = "applyToValue";
  public static final String APPLY_TO_FIELD_CONFIG = "applyToField";
  public static final Validator APPLY_TO_VALIDATOR = new Validator() {

    private Pattern pattern = Pattern.compile("(?i)^key$|^value$|^topic$|^header\\..*$");

    @Override
    public void ensureValid(String name, Object value) {
      if (value == null)
        throw new ConfigException(name, value, "'applyTo' must be list of one of:  KEY, VALUE, TOPIC, HEADER.<header_name>");
      List<String> list = (List<String>) value;
      if (list.size() == 0)
        throw new ConfigException(name, value, "'applyTo' must contain a value of:  KEY, VALUE, TOPIC, HEADER.<header_name>");

      for (String str : list) {
        if (str == null || StringUtils.isBlank(str) || !pattern.matcher(str).matches()) {
          throw new ConfigException(name, value, "applyTo value must be one of:  KEY, VALUE, HEADER.<header_name>");
        }
      }
    }
  };

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(APPLY_TO_CONFIG, ConfigDef.Type.LIST, APPLY_TO_DEFAULT, APPLY_TO_VALIDATOR, ConfigDef.Importance.HIGH, "Where to apply transformation. Supported values: KEY, VALUE, TOPIC, HEADER.<header_name>")
      .define(APPLY_TO_VALUE_CONFIG, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.MEDIUM, "value regex will be applied to the result of EL")
      .define(APPLY_TO_FIELD_CONFIG, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.MEDIUM, "in case of structured data regex will be applied to field value");

  private static final Pattern HDR_PATTERN = Pattern.compile("(?i)^header\\.(.*)$");

  protected List<String> applyToList;
  private String applyToValue;
  private String applyToField;

  @Override
  public void configure(Map<String, ?> configs) {

    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    this.applyToList = config.getList(APPLY_TO_CONFIG);
    this.applyToValue = config.getString(APPLY_TO_VALUE_CONFIG);
    this.applyToField = config.getString(APPLY_TO_FIELD_CONFIG);
    
    //'normalize' applyTo values: uppercase and HEADER.Name
    applyToList.replaceAll(s -> {
      if (HDR_PATTERN.matcher(s).matches()) {
        return HDR_PATTERN.matcher(s).replaceAll("HEADER.$1");
      } else {
        return s.trim().toUpperCase();
      }
    });
  }

  /**
   * Returns current value(s) for 'applyTo' from record
   * The reason why List is here because header may contain multiply value
   * 
   * @param applyTo
   * @param record
   * @return
   */
  protected List<Object> valuesFor(String applyTo, R record) {
    if (record == null)
      return null;

    //if applyToField defined record.value will be handled as structured data and used instead of 'applyTo' 
    if (StringUtils.isNotBlank(applyToField)) {
      Object value = record.value();

      String data;
      if (value instanceof Struct) {
        data = readDataFromStruct((Struct) value);
      } else if (value instanceof Map) {
        data = readDataFromMap((Map) value);
      } else {
        data = readDataFromString(value.toString());
      }
      return Arrays.asList(data); //TODO refactor methods 'readDataFrom*' to support multiply values
    }

    //if applyToValue defined use  EL result instead of 'applyTo'
    if (StringUtils.isNotBlank(applyToValue)) {
      List<Object> data;
      if (ELUtils.containsEL(applyToValue)) {
        data = Arrays.asList(ELUtils.getExprValue(applyToValue, record));
      } else {
        data = Arrays.asList(applyToValue);
      }
      return data;
    }

    if ("VALUE".equals(applyTo)) {
      if (record.value() == null) {
        return null;
      } else {
        return Arrays.asList(record.value());
      }
    } else if ("KEY".equals(applyTo)) {
      if (record.key() == null) {
        return null;
      } else {
        return Arrays.asList(record.key());
      }
    } else if ("TOPIC".equals(applyTo)) {
      return Arrays.asList(record.topic());
    } else if (applyTo.startsWith("HEADER.")) {
      Iterator<Header> hdrI = record.headers().allWithName(applyTo.substring(7));
      List<Object> values = new ArrayList<>();
      while (hdrI.hasNext()) {
        Object value = hdrI.next().value();
        if (value != null)
          values.add(value);
      }
      return values;
    } else if (ELUtils.containsEL(applyTo)) {
      return Arrays.asList(ELUtils.getExprValue(applyTo, record));
    } else {
      return Arrays.asList(applyTo);
    }
  }

  private String readDataFromMap(Map mapValue) {
    Object value = mapValue.get(applyToField);
    return value == null ? null : value.toString();
  }

  private String readDataFromStruct(Struct structValue) {
    Object value = structValue.get(applyToField);
    return value == null ? null : value.toString();
  }

  private String readDataFromString(String stringData) {
    try {
      // make attempt to read string as json
      return readDataFromJson(OBJECT_MAPPER.readTree(stringData));
    } catch (Exception e) {
      log.debug("Failed to convert to JSON", e);
    }

    return stringData;
  }

  private String readDataFromJson(JsonNode jsonValue) {
    try {
      return jsonValue.at(applyToField).asText();
    } catch (Exception e) {
      log.warn("Wrong data expression: {}", applyToField);
    }

    return jsonValue.toString();
  }

  /**
   * Build new record from tgransformed valued
   * @param record
   * @param newValues
   * @return
   */
  protected R buildNewRecord(R record, Map<String, List<Object>> newValues) {
    if (newValues == null || newValues.size() == 0) {
      return record;
    }

    R newRec = record;
    //skip record duplication if key,value,topic was not changed 
    if (newValues.containsKey("TOPIC") ||
        newValues.containsKey("KEY") ||
        newValues.containsKey("VALUE")) {

      //create new Record with modified values
      newRec = record.newRecord(
          newValues.containsKey("TOPIC") ? newValues.get("TOPIC").get(0).toString() : record.topic(),
          record.kafkaPartition(),
          record.keySchema(),
          newValues.containsKey("KEY") ? newValues.get("KEY").get(0) : record.key(),
          record.valueSchema(),
          newValues.containsKey("VALUE") ? newValues.get("VALUE").get(0) : record.value(),
          record.timestamp());
      newValues.remove("TOPIC");
      newValues.remove("KEY");
      newValues.remove("VALUE");
    }

    //NOW: replaces header values
    //TODO: think about add/replace header option
    for (Entry<String, List<Object>> e : newValues.entrySet()) {
      if (e.getKey().startsWith("HEADER.")) {
        String hdrName = e.getKey().substring(7);
        if (newRec.headers() != null && !newRec.headers().isEmpty()) {
          newRec.headers().remove(hdrName);
        }
        for (Object hdrValue : e.getValue()) {
          //TODO: implement schema support to avoid hdrValue.toString()
          newRec.headers().addString(hdrName, hdrValue.toString());
        }
      }
    }
    //TODO: implement support for applyTo=FIELD.* to support update of structured data
    return newRec;
  }

}
