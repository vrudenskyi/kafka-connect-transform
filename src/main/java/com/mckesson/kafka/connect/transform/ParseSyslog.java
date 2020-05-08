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

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mckesson.kafka.connect.utils.SyslogParser;

public class ParseSyslog<R extends ConnectRecord<R>> implements Transformation<R> {

  static final Logger log = LoggerFactory.getLogger(ParseSyslog.class);
  
  public static final ConfigDef CONFIG_DEF = new ConfigDef();

  @Override
  public void configure(Map<String, ?> configs) {
  }

  @Override
  public R apply(R record) {

    Object recordValue = record.value();
    if (recordValue == null) {
      return record;
    }

    Map<String, Object> syslogData = Collections.emptyMap();
    try {
      if (recordValue instanceof String) {
        syslogData = SyslogParser.parseMessage((String) recordValue);
      } else if (recordValue instanceof byte[]) {
        syslogData = SyslogParser.parseMessage((byte[]) recordValue);
      } else {
        log.debug("value of class {}  is not supported. String or byte[] supported, return as-is",
            recordValue.getClass().getName());
        return record;
      }
    } catch (Exception e1) {
      log.warn("Failed to read json from record data, returned as-is. Data: {}", recordValue, e1);
      return record;
    }

    R newRec = record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), syslogData.get(SyslogParser.SYSLOG_LOG_MESSAGE), record.timestamp(), record.headers());
    //put as headers all except SYSLOG_LOG_MESSAGE
    syslogData.forEach((k, v) -> {
      if (!SyslogParser.SYSLOG_LOG_MESSAGE.equals(k))
        record.headers().addString("syslog." + k, (v == null ? "" : v.toString()));
    });

    return newRec;

  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
  }

}
