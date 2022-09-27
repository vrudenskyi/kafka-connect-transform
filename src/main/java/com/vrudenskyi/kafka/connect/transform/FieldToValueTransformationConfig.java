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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class FieldToValueTransformationConfig extends AbstractConfig {

  
  public static final String FIELD_MESSAGE_CONFIG = "field.message";
  static final String FIELD_MESSAGE_DOC = "The field that stores the message.";
  static final String FIELD_MESSAGE_DEFAULT = "message";


  public final String fieldMessage;
  

  public FieldToValueTransformationConfig(Map<String, ?> parsedConfig) {
    super(config(), parsedConfig);
    this.fieldMessage = getString(FIELD_MESSAGE_CONFIG);
    
  }

  static ConfigDef config() {
    return new ConfigDef()
        .define(FIELD_MESSAGE_CONFIG, ConfigDef.Type.STRING, FIELD_MESSAGE_DEFAULT, ConfigDef.Importance.HIGH, FIELD_MESSAGE_DOC);
  }  
}
