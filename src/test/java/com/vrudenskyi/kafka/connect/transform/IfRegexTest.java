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

import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Test;

import com.vrudenskyi.kafka.connect.transform.IfRegex;

public class IfRegexTest {
  
  
  @Test
  public void testIfModeConfig() {
    
    IfRegex ifRegex = new IfRegex();
    Map<String, Object> ifConf = new HashMap<>();
    ifConf.put(IfRegex.IF_MODE_CONFIG, "xxx");
    try {
      ifRegex.configure(ifConf);
      Assert.fail();
    } catch (ConfigException e) {
    }
    
    ifConf.put(IfRegex.IF_MODE_CONFIG, "eq");
    ifRegex.configure(ifConf);
    
    ifConf.put(IfRegex.IF_MODE_CONFIG, "Eq");
    ifRegex.configure(ifConf);

    
    ifConf.put(IfRegex.IF_MODE_CONFIG, "EQ");
    ifRegex.configure(ifConf);
    
    
    ifConf.put(IfRegex.IF_MODE_CONFIG, "find");
    ifRegex.configure(ifConf);

    
    ifConf.put(IfRegex.IF_MODE_CONFIG, "Find");
    ifRegex.configure(ifConf);
    
    ifConf.put(IfRegex.IF_MODE_CONFIG, "In");
    ifRegex.configure(ifConf);
    
    ifConf.put(IfRegex.IF_MODE_CONFIG, "IN");
    ifRegex.configure(ifConf);
    
    
  }
  
  @Test
  public void testEq() {
    
    IfRegex ifRegex = new IfRegex();
    Map<String, Object> ifConf = new HashMap<>();
    ifConf.put(IfRegex.IF_MODE_CONFIG, "eq");
    ifConf.put(IfRegex.IF_CONFIG, "Test");
        ifRegex.configure(ifConf);
    
    Assert.assertTrue(ifRegex.checkIf("Test"));
    Assert.assertFalse(ifRegex.checkIf("test"));
    Assert.assertFalse(ifRegex.checkIf("xxx"));
    
  }
  
  
  @Test
  public void testIn() {
    
    IfRegex ifRegex = new IfRegex();
    Map<String, Object> ifConf = new HashMap<>();
    ifConf.put(IfRegex.IF_MODE_CONFIG, "in");
    ifConf.put(IfRegex.IF_CONFIG, "111,222,333,xxx,555");
        ifRegex.configure(ifConf);
    
    Assert.assertTrue(ifRegex.checkIf("111"));
    Assert.assertTrue(ifRegex.checkIf("xxx"));
    Assert.assertTrue(ifRegex.checkIf("555"));
    Assert.assertFalse(ifRegex.checkIf("3333"));
    Assert.assertFalse(ifRegex.checkIf("yyyy"));
    
  }
  



}
