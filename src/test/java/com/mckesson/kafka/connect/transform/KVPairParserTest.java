package com.mckesson.kafka.connect.transform;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

public class KVPairParserTest {

  @Test
  public void simpleWithDefaultsTest() {
    String msg = "k1=v1 k2=v2 k3=\"v3\"";

    SinkRecord rec = new SinkRecord("testTopic", 0, null, null, null, msg, 0l);

    //init transormer
    KVPairParser t = new KVPairParser();
    Map<String, String> tConf = new HashMap<>();
    t.configure(tConf);

    SinkRecord resultRec = (SinkRecord) t.apply(rec);
    t.close();
    
    Assert.assertNotNull(resultRec.value());
    Assert.assertTrue(resultRec.value() instanceof Map);
    Assert.assertTrue(((Map) resultRec.value()).size() == 3);

    Assert.assertTrue(((Map) resultRec.value()).get("k1").equals("v1"));
    Assert.assertTrue(((Map) resultRec.value()).get("k2").equals("v2"));
    Assert.assertTrue(((Map) resultRec.value()).get("k3").equals("v3"));

  }
  
  @Test
  public void simpleWithDataHeaderTest() {
    String msg = "test data header k1=v1 k2=v2 k3=\"v3\"";

    SinkRecord rec = new SinkRecord("testTopic", 0, null, null, null, msg, 0l);

    //init transormer
    KVPairParser t = new KVPairParser();
    Map<String, String> tConf = new HashMap<>();
    tConf.put(KVPairParser.HEADER_SPLIT_REGEXP_CONFIG, "\\s(?=\\w+\\=)");//last space before 'key='
    t.configure(tConf);

    SinkRecord resultRec = (SinkRecord) t.apply(rec);
    t.close();
    
    Assert.assertNotNull(resultRec.value());
    Assert.assertTrue(resultRec.value() instanceof Map);
    System.out.println(resultRec.value());
    Assert.assertTrue(((Map) resultRec.value()).size() == 4);

    Assert.assertTrue(((Map) resultRec.value()).get("k1").equals("v1"));
    Assert.assertTrue(((Map) resultRec.value()).get("k2").equals("v2"));
    Assert.assertTrue(((Map) resultRec.value()).get("k3").equals("v3"));
    Assert.assertTrue(((Map) resultRec.value()).get("DATA_HEADER").equals("test data header"));

  }

  
  @Test
  public void simpleCustomDelimitersTest() {
    String msg = "k1:v1, k2:v2, k3:\"v3\"";

    SinkRecord rec = new SinkRecord("testTopic", 0, null, null, null, msg, 0l);

    //init transormer
    KVPairParser t = new KVPairParser();
    Map<String, String> tConf = new HashMap<>();
    tConf.put(KVPairParser.PAIR_DELIMITER_CONFIG, ",");
    tConf.put(KVPairParser.KV_DELIMITER_CONFIG, ":");
    t.configure(tConf);

    SinkRecord resultRec = (SinkRecord) t.apply(rec);
    t.close();

    Assert.assertNotNull(resultRec.value());
    Assert.assertTrue(resultRec.value() instanceof Map);
    Assert.assertTrue(((Map) resultRec.value()).size() == 3);

    Assert.assertTrue(((Map) resultRec.value()).get("k1").equals("v1"));
    Assert.assertTrue(((Map) resultRec.value()).get("k2").equals("v2"));
    Assert.assertTrue(((Map) resultRec.value()).get("k3").equals("v3"));

  }

  
  
  

  @Test
  public void simpleCEFTest() {
    String msg = "CEF:0|Incapsula|SIEMintegration|1|1|Normal|0| fileId=486000240000010094 sourceServiceName=sssss siteid=59162523 suid=93951 deviceFacility=iad";
    SinkRecord rec = new SinkRecord("testTopic", 0, null, null, null, msg, 0l);

    //init transormer
    CEF2Map t = new CEF2Map();
    Map<String, String> tConf = new HashMap<>();
    t.configure(tConf);

    SinkRecord resultRec = (SinkRecord) t.apply(rec);
    t.close();
    Assert.assertNotNull(resultRec.value());
    Assert.assertTrue(resultRec.value() instanceof Map);
    Assert.assertTrue(((Map) resultRec.value()).size() == 6);
    Assert.assertTrue(((Map) resultRec.value()).get("DATA_HEADER").equals("CEF:0|Incapsula|SIEMintegration|1|1|Normal|0|"));
    Assert.assertTrue(((Map) resultRec.value()).get("sourceServiceName").equals("sssss"));
    Assert.assertTrue(((Map) resultRec.value()).get("deviceFacility").equals("iad"));
    Assert.assertTrue(((Map) resultRec.value()).get("siteid").equals("59162523"));
  }

}
