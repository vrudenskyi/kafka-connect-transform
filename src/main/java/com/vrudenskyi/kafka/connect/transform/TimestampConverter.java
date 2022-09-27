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

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import com.vrudensk.kafka.connect.utils.DateTimeUtils;
import com.vrudensk.kafka.connect.utils.ELUtils;

public class TimestampConverter<R extends ConnectRecord<R>> extends AppyToTransformer<R> implements Configurable {

  private static final String TYPE_STRING = "string";
  private static final String TYPE_EPOCH = "epoch";

  public static final String TARGET_TYPE_CONFIG = "target.type";
  public static final String TARGET_TYPE_DEFAULT = TYPE_STRING;

  public static final String TARGET_PATTERN_CONFIG = "target.pattern";
  public static final String TARGET_PATTERN_DEFAULT = "yyyy-MM-dd'T'HH:mm:ss.SSSX";

  public static final String TARGET_ZONE_ID_CONFIG = "target.zoneId";
  public static final String TARGET_ZONE_ID_DEFAULT = "UTC";

  public static final String SOURCE_TYPE_CONFIG = "source.type";
  public static final String SOURCE_TYPE_DEFAULT = TYPE_STRING;

  public static final String SOURCE_DEFAULT_CONFIG = "source.default.ms";
  public static final long SOURCE_DEFAULT_DEFAULT = -1;

  public static final String TARGET_DEFAULT_CONFIG = "target.default.ms";
  public static final long TARGET_DEFAULT_DEFAULT = -1;

  public static final String SOURCE_PATTERN_CONFIG = "source.pattern";
  public static final String SOURCE_PATTERN_DEFAULT = "yyyy-MM-dd'T'HH:mm:ss.SSSX";

  public static final String SOURCE_ZONE_ID_CONFIG = "source.zoneId";
  public static final String SOURCE_ZONE_ID_DEFAULT = "UTC";

  public static final String IGNORE_PARSE_ERROR_CONFIG = "source.ignore.parse.error";
  public static final boolean IGNORE_PARSE_ERROR_DEFAULT = false;

  public static final String CONDITION_CONFIG = "condition";

  private String sourceType;
  private DateTimeFormatter sourceFormat;
  private long sourceDefault;

  private String targetType;
  private DateTimeFormatter targetFormat;
  private long targetDefault;

  private IfRegex ifRegex;
  private boolean ignoreParseError;

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(SOURCE_TYPE_CONFIG, ConfigDef.Type.STRING, SOURCE_TYPE_DEFAULT, ConfigDef.Importance.MEDIUM, "Source timestamp format: string or epoch")
      .define(SOURCE_PATTERN_CONFIG, ConfigDef.Type.STRING, SOURCE_PATTERN_DEFAULT, ConfigDef.Importance.MEDIUM, "Pattern for string source")

      .define(TARGET_TYPE_CONFIG, ConfigDef.Type.STRING, TARGET_TYPE_DEFAULT, ConfigDef.Importance.MEDIUM, "Target timestamp format: string or epoch")
      .define(TARGET_PATTERN_CONFIG, ConfigDef.Type.STRING, TARGET_PATTERN_DEFAULT, ConfigDef.Importance.MEDIUM, "Pattern for string target")

      .define(SOURCE_DEFAULT_CONFIG, ConfigDef.Type.LONG, SOURCE_DEFAULT_DEFAULT, ConfigDef.Importance.LOW, "Default value in millis if source empty")
      .define(TARGET_DEFAULT_CONFIG, ConfigDef.Type.LONG, TARGET_DEFAULT_DEFAULT, ConfigDef.Importance.LOW, "Default target value in millis if failed to parse source. default: now")
      .define(SOURCE_ZONE_ID_CONFIG, ConfigDef.Type.STRING, SOURCE_ZONE_ID_DEFAULT, ConfigDef.Importance.LOW, "Default ZoneId for source")
      .define(TARGET_ZONE_ID_CONFIG, ConfigDef.Type.STRING, TARGET_ZONE_ID_DEFAULT, ConfigDef.Importance.LOW, "Default ZoneId for target")
      .define(IGNORE_PARSE_ERROR_CONFIG, ConfigDef.Type.BOOLEAN, IGNORE_PARSE_ERROR_DEFAULT, ConfigDef.Importance.LOW, "Ignore parse exception");

  @Override
  public void configure(Map<String, ?> props) {
    super.configure(props);
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

    sourceType = config.getString(SOURCE_TYPE_CONFIG);
    sourceFormat = DateTimeFormatter.ofPattern(config.getString(SOURCE_PATTERN_CONFIG)).withZone(ZoneId.of(config.getString(SOURCE_ZONE_ID_CONFIG)));

    targetType = config.getString(TARGET_TYPE_CONFIG);
    targetFormat = DateTimeFormatter.ofPattern(config.getString(TARGET_PATTERN_CONFIG)).withZone(ZoneId.of(config.getString(TARGET_ZONE_ID_CONFIG)));

    sourceDefault = config.getLong(SOURCE_DEFAULT_CONFIG);
    targetDefault = config.getLong(TARGET_DEFAULT_CONFIG);

    ignoreParseError = config.getBoolean(IGNORE_PARSE_ERROR_CONFIG);

    this.ifRegex = new IfRegex();
    Map<String, Object> conditionConfig = config.originalsWithPrefix(CONDITION_CONFIG + ".");
    if (conditionConfig.size() > 0) {
      this.ifRegex.configure(conditionConfig);

    }

  }

  @Override
  public R apply(R record) {

    Map<String, List<Object>> newValues = new HashMap<String, List<Object>>(applyToList.size());
    //read currentValues and transform
    for (String applyTo : applyToList) {
      List<Object> current = valuesFor(applyTo, record);
      if (current != null) {
        List<Object> transformed = new ArrayList<>(current.size());
        boolean nothingTransformed = true;
        //check condition first
        for (Object value : current) {
          if (!ifRegex.ifConfigured() || ifRegex.checkIf(ELUtils.getExprValue(ifRegex.getIfData(value.toString()), record))) {
            transformed.add(value == null ? null : transformTimestamp(value));
            nothingTransformed = false;
          } else {
            transformed.add(value == null ? null : value);
          }
        }
        if (!nothingTransformed) {
          //if nothing in fact was transformed skip curent 'applyTo' value 
          newValues.put(applyTo, transformed);
        }

      }
    }
    return buildNewRecord(record, newValues);
  }

  private Object transformTimestamp(Object value) {

    Instant ts;
    if (value == null || StringUtils.isBlank(value.toString())) {
      ts = this.sourceDefault < 0 ? Instant.now() : Instant.ofEpochMilli(sourceDefault);
    } else {
      if (TYPE_EPOCH.equals(sourceType)) {
        ts = Instant.ofEpochMilli(Long.valueOf(value.toString()));
      } else {
        try {
          ts = ZonedDateTime.parse(value.toString(), sourceFormat).toInstant();
        } catch (DateTimeParseException e) {
          //try to parse with utils which 'knows' more formats, otherwise through original exception
          try {
            ts = DateTimeUtils.parseZonedDateTime(value.toString()).toInstant();
          } catch (Exception e1) {
            if (!ignoreParseError) {
              throw e;
            }
            ts = this.targetDefault < 0 ? Instant.now() : Instant.ofEpochMilli(targetDefault);
          }
        }
      }
    }

    if (TYPE_EPOCH.equals(targetType)) {
      return Long.valueOf(ts.toEpochMilli());
    } else {
      return targetFormat.format(ts);
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
