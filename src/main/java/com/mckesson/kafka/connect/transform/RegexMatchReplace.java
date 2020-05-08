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
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class RegexMatchReplace extends IfRegex implements Configurable {

  public static final String FIND_CONFIG = "find";
  public static final String REPLACEMENT_CONFIG = "replacement";

  public static final String OVERVIEW_DOC = "Replace string using the configured regular expression and replacement string. If condition can be also checked before replacement"
      + "<p/>Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. ";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(FIND_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
          "Regex for find-replace")
      .define(REPLACEMENT_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH,
          "Replacement string.");

  private Pattern findPattern;
  private String replacement;

  @Override
  public void configure(Map<String, ?> props) {
    super.configure(props);
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    String findString = config.getString(FIND_CONFIG);
    if (StringUtils.isNoneBlank(findString)) {
      findPattern = Pattern.compile(config.getString(FIND_CONFIG));
    }

    replacement = config.getString(REPLACEMENT_CONFIG);
  }

  public String apply(String message) {

    //apply if no conditions or matches
    boolean doApply = !ifConfigured() || checkIf(message);

    if (doApply) {
      if (findPattern != null) {
        return findPattern.matcher(message).replaceAll(replacement);
      }
      return replacement;

    }
    return message;

  }

  public String getReplacement() {
    return replacement;
  }

}
