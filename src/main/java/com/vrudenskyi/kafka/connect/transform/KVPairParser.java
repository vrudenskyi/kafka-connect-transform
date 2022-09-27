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

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KVPairParser<R extends ConnectRecord<R>> implements Transformation<R> {

  static final Logger log = LoggerFactory.getLogger(CEF2Map.class);

  public static final String PAIR_DELIMITER_CONFIG = "pairDelimiter";

  public static final String KV_DELIMITER_CONFIG = "kvDelimiter";
  private static final String KV_DELIMITER_DEFAULT = "=";

  public static final String HEADER_SPLIT_REGEXP_CONFIG = "headerSplitRegexp";

  public static final String QUOTES_SYMBOL_CONFIG = "quotesSymbol";
  private static final String QUOTES_SYMBOL_DEFAULT = "\"";

  public static final String ESCAPE_SYMBOL_CONFIG = "escapeSymbol";
  private static final String ESCAPE_SYMBOL_DEFAULT = "\\";

  public static final String FIELD_PREFIX_CONFIG = "fieldPrefix";
  private static final String FIELD_PREFIX_DEFAULT = "";

  public static final String HEADER_FIELD_NAME_CONFIG = "headerName";
  private static final String HEADER_FIELD_NAME_DEFAULT = "DATA_HEADER";

  public static final String ADD_EMPTY_VALUES_CONFIG = "addEmptyValues";
  private static final Boolean ADD_EMPTY_VALUES_DEFAULT = Boolean.TRUE;

  public static final String TRIM_VALUES_CONFIG = "trimValues";
  private static final Boolean TRIM_VALUES_DEFAULT = Boolean.TRUE;

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(PAIR_DELIMITER_CONFIG, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.MEDIUM, "delimiter of kv pairs. default: <space>")
      .define(KV_DELIMITER_CONFIG, ConfigDef.Type.STRING, KV_DELIMITER_DEFAULT, null, ConfigDef.Importance.MEDIUM, "key and value delimiter. default: =")
      .define(HEADER_SPLIT_REGEXP_CONFIG, ConfigDef.Type.STRING, null, null, ConfigDef.Importance.MEDIUM, "header and data spolit regexp")
      .define(QUOTES_SYMBOL_CONFIG, ConfigDef.Type.STRING, QUOTES_SYMBOL_DEFAULT, null, ConfigDef.Importance.MEDIUM, "quotes symbol")
      .define(ESCAPE_SYMBOL_CONFIG, ConfigDef.Type.STRING, ESCAPE_SYMBOL_DEFAULT, null, ConfigDef.Importance.MEDIUM, "escape symbol")
      .define(TRIM_VALUES_CONFIG, ConfigDef.Type.BOOLEAN, TRIM_VALUES_DEFAULT, null, ConfigDef.Importance.MEDIUM, "trim values")

      .define(FIELD_PREFIX_CONFIG, ConfigDef.Type.STRING, FIELD_PREFIX_DEFAULT, null, ConfigDef.Importance.LOW, "add prefix to output field names")
      .define(HEADER_FIELD_NAME_CONFIG, ConfigDef.Type.STRING, HEADER_FIELD_NAME_DEFAULT, null, ConfigDef.Importance.LOW, "name of header field")
      .define(ADD_EMPTY_VALUES_CONFIG, ConfigDef.Type.BOOLEAN, ADD_EMPTY_VALUES_DEFAULT, null, ConfigDef.Importance.MEDIUM, "add empty values. default: true");

  private String pairDelimiter;
  private String kvDelimiter;
  private String quotesSymbol;
  private String escapeSymbol;
  private String outputFieldPrefix;
  private String outputHeaderName;
  private boolean addEmptyValues;
  private boolean trimValues;
  private String separator;
  private String headerSplitRegexp;

  public KVPairParser() {
    super();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    pairDelimiter = conf.getString(PAIR_DELIMITER_CONFIG);
    if (StringUtils.isBlank(pairDelimiter)) {
      pairDelimiter = " ";
    }
    kvDelimiter = conf.getString(KV_DELIMITER_CONFIG);
    quotesSymbol = conf.getString(QUOTES_SYMBOL_CONFIG);
    escapeSymbol = conf.getString(ESCAPE_SYMBOL_CONFIG);
    outputFieldPrefix = conf.getString(FIELD_PREFIX_CONFIG);
    outputHeaderName = conf.getString(HEADER_FIELD_NAME_CONFIG);
    addEmptyValues = conf.getBoolean(ADD_EMPTY_VALUES_CONFIG);
    trimValues = conf.getBoolean(TRIM_VALUES_CONFIG);
    headerSplitRegexp = conf.getString(HEADER_SPLIT_REGEXP_CONFIG);

    this.separator = kvDelimiter + escapeSymbol + quotesSymbol;

  }

  @Override
  public R apply(R record) {

    Object recordValue = record.value();
    if (recordValue == null) {
      return record;
    }

    String stringData;
    try {
      if (recordValue instanceof String) {
        stringData = (String) recordValue;
      } else if (recordValue instanceof byte[]) {
        stringData = new String((byte[]) recordValue);
      } else {
        log.debug("value of class {}  is not supported. String or byte[] supported, return as-is",
            recordValue.getClass().getName());
        return record;
      }
    } catch (Exception e1) {
      log.warn("Failed to read json from record data, returned as-is. Data: {}", recordValue, e1);
      return record;
    }

    Map<String, String> map = new HashMap<>();
    String[] hdrAndData = splitHeaderAndData(stringData);
    String dataString;
    if (hdrAndData.length > 1) {
      map.put(outputHeaderName, hdrAndData[0]);
      dataString = hdrAndData[1];
    } else {
      dataString = hdrAndData[0];
    }

    StringTokenizer st = new StringTokenizer(dataString, separator, true);

    String key = nextToken(st);

    String value;
    while (st.hasMoreTokens()) {
      String nextToken = nextToken(st);
      int lastIndexOfDelimiter = nextToken.lastIndexOf(pairDelimiter);
      lastIndexOfDelimiter = lastIndexOfDelimiter == -1 ? nextToken.length() : lastIndexOfDelimiter;
      value = (st.hasMoreTokens() ? nextToken.substring(0, lastIndexOfDelimiter) : nextToken);
      if (value.length() > 0 || addEmptyValues) {
        map.put(concat(outputFieldPrefix, trim(key)),
            unquote(StringUtils.stripEnd(value, pairDelimiter)));
      }

      if (lastIndexOfDelimiter != -1) {
        key = nextToken.substring(lastIndexOfDelimiter).replaceAll(pairDelimiter, "");
      }
    }

    return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
        record.valueSchema(), map, record.timestamp());

  }

  private String nextToken(StringTokenizer st) {
    return nextToken(st, null, false);
  }

  private String nextToken(StringTokenizer st, String currentToken, boolean quoted) {

    if (!st.hasMoreTokens())
      return currentToken;

    String nextToken = st.nextToken();
    //<QUOT> but not <ESC><QUOT> => switch quoted flag
    if (quotesSymbol.contains(nextToken) && (currentToken == null || !currentToken.endsWith(escapeSymbol))) {
      quoted = !quoted;

    }

    //<QUOTED> OR <ESC> or <ESC><EQ> ==> keep reading     
    if (quoted || (currentToken != null && escapeSymbol.equals(nextToken)) || (currentToken != null && currentToken.endsWith(escapeSymbol) && kvDelimiter.equals(nextToken))) {
      return nextToken(st, (currentToken == null ? nextToken : currentToken + nextToken), quoted);
    }

    //<EQ> not part of escape
    if (kvDelimiter.equals(nextToken)) {
      return currentToken;
    }

    return nextToken(st, currentToken == null ? nextToken : currentToken + nextToken, quoted);
  }

  protected String[] splitHeaderAndData(String data) {
    if (StringUtils.isBlank(headerSplitRegexp)) {
      return new String[] {data};
    }
    return data.split(headerSplitRegexp, 2);
  }

  private String unquote(String str) {
    if (StringUtils.isNotBlank(quotesSymbol) && str.startsWith(quotesSymbol) && str.endsWith(quotesSymbol)) {
      return str.substring(quotesSymbol.length(), str.length() - quotesSymbol.length());
    }
    return str;
  }

  private String trim(String str) {
    return trimValues ? str.trim() : str;
  }

  private String concat(String x, String y) {
    return x.length() == 0 ? y : x + y;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

}