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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import com.mckesson.kafka.connect.utils.ConfigUtils;

/**
 * Regexp based 'if' condition  
 * 
 */
public class IfRegex implements Configurable {

  public static final String IF_CONFIG = "if";
  public static final String IF_MODE_CONFIG = "if_mode";
  public static final String IF_MODE_DEFAULT = IfMode.FIND.name();
  /*for future use  to me able to implement method: boolean if*(ConnectRecord)*/
  public static final String IF_DATA_CONFIG = "if_data";
  public static final String IF_DATA_DEFAULT = null;

  
  private String ifString;
  private Pattern ifPattern;
  private Set<String> ifSet;

  
  private IfMode ifMode;
  private String ifData;

  
  private enum IfMode {
    MATCH, FIND, EQ, CONTAIN, IN
  };

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(IF_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Regexp or value for if condition")
      .define(IF_MODE_CONFIG, ConfigDef.Type.STRING, IF_MODE_DEFAULT, ConfigUtils.validEnum(IfMode.class), ConfigDef.Importance.LOW,
          "if condition mode: 'match', 'find', 'contain', Default: 'find'")
      .define(IF_DATA_CONFIG, ConfigDef.Type.STRING, IF_DATA_DEFAULT, ConfigDef.Importance.LOW, "EL for data");

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

    ifMode = ConfigUtils.getEnum(config, IF_MODE_CONFIG, IfMode.class);
    ifString = config.getString(IF_CONFIG);
    ifData = config.getString(IF_DATA_CONFIG);
    

    if (StringUtils.isNoneBlank(ifString) && (ifMode.equals(IfMode.FIND) || ifMode.equals(IfMode.MATCH))) {
      ifPattern = Pattern.compile(ifString);
    }
    
    if (StringUtils.isNoneBlank(ifString) && ifMode.equals(IfMode.IN)) {
      ifSet = new HashSet((List<String>) ConfigDef.parseType(IF_CONFIG, ifString, Type.LIST));
    }
    

  }

  public boolean ifConfigured() {
    return ifString != null;
  }

  public boolean checkIf(String message) {
    boolean result = false;
    switch (ifMode) {
      case EQ:
        result = ifEq(message);
        break;

      case CONTAIN:
        result = ifContains(message);
        break;

      case MATCH:
        result = ifMatches(message);
        break;

      case FIND:
        result = ifFind(message);
        break;
        
      case IN:
        result = ifIn(message);
        break;
        
    }

    return result;

  }
  
  private boolean ifIn(String message) {
    return ifSet != null && StringUtils.isNotBlank(message) &&  ifSet.contains(message);
  }

  private boolean ifContains(String message) {
    return ifString != null && StringUtils.isNotBlank(message) && message.contains(ifString);
  }

  private boolean ifMatches(String message) {
    return ifPattern != null && ifPattern.matcher(message).matches();
  }

  private boolean ifEq(String message) {
    return ifString != null && ifString.equals(message);
  }

  private boolean ifFind(String message) {
    return ifPattern != null && ifPattern.matcher(message).find();
  }

  public String getIfData(String defaultValue) {
    return ifData != null ? ifData : defaultValue;
  }
  
  public String getIfData() {
    return getIfData("${VALUE}");
  }
}