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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.util.StdDateFormat;

/**
 * Transforms any supported format to JSON
 * supports any format natively supported  + {@link Struct}  
 * 
 * @param <R>
 */
public class ToJson<R extends ConnectRecord<R>> implements Transformation<R> {
  
  public static final ConfigDef CONFIG_DEF = new ConfigDef();

  static final Logger log = LoggerFactory.getLogger(ToJson.class);

  static class StructModule extends SimpleModule {
    public StructModule() {
      addSerializer(Struct.class, new StructSerializer());
    }

    class StructSerializer extends JsonSerializer<Struct> {
      @Override
      public void serialize(Struct struct, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        final Map<String, Object> result = new LinkedHashMap<>(struct.schema().fields().size());
        for (Field field : struct.schema().fields()) {
          result.put(field.name(), struct.get(field));
        }
        jsonGenerator.writeObject(result);
      }
    }
  }

  static final ObjectMapper JSON_MAPPER;
  static {
    JSON_MAPPER = new ObjectMapper();
    JSON_MAPPER.registerModule(new StructModule());
    JSON_MAPPER.setDateFormat(StdDateFormat.instance);
  }

  private static final List<Schema.Type> SUPPORTED_VALUE_SCHEMA_TYPES;
  static {
    SUPPORTED_VALUE_SCHEMA_TYPES = Collections.unmodifiableList(Arrays.asList(Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.STRUCT));
  }

  @Override
  public void configure(Map<String, ?> configs) {
    //TODO implement jsonMapper options
  }

  @Override
  public R apply(R r) {

    Object recordValue = r.value();
    if (recordValue == null) {
      return r;
    }

    detectUnsupportedSchemaType(r.valueSchema());

    try {
      return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), Schema.STRING_SCHEMA, JSON_MAPPER.writeValueAsString(r.value()), r.timestamp());
    } catch (JsonProcessingException e) {
      log.trace("Failed to convert");
      r.headers().addString("error", e.getMessage());
    }
    return r;
  }

  private void detectUnsupportedSchemaType(Schema schema) {
    if (!SUPPORTED_VALUE_SCHEMA_TYPES.contains(schema.type())) {
      String specified;

      if (schema.name() != null) {
        specified = String.format("%s-%s", schema.type(), schema.name());
      } else {
        specified = String.format("%s", schema.type());
      }
      String message = String.format("Only supported schema types can be converted to JSON; specified [%s], supported %s", specified, SUPPORTED_VALUE_SCHEMA_TYPES);

      throw new ConnectException(message);
    }

  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

}
