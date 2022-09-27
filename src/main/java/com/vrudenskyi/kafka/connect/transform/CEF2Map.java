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

import org.apache.kafka.connect.connector.ConnectRecord;

public class CEF2Map<R extends ConnectRecord<R>> extends KVPairParser<R> {

  //find where 'Extension' begins CEF has  up to 7 fields in Prefix
  //CEF:Version|Device Vendor|Device Product|Device Version|SignatureID|Name|Severity|Extension
  @Override
  protected String[] splitHeaderAndData(String data) {
    int idx = data.indexOf("CEF:");
    for (int i = 0; i < 7; i++) {
      int p = data.indexOf('|', idx + 1);
      if (p == -1) {
        break;
      }
      idx = p;
    }

    if (idx > 0) {
      return new String[] {data.substring(0, idx + 1), data.substring(idx + 1)};
    } else {
      return new String[] {data};
    }

  }

}
