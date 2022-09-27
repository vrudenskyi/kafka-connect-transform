package com.vrudenskyi.kafka.connect.transform;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import com.vrudenskyi.kafka.connect.transform.IfRegex;
import com.vrudenskyi.kafka.connect.transform.TimestampConverter;

public class TimestampConverterTest {
  
  @Test
  public void testWithoutTZ() {
    String msg = "kkk";
    
    SinkRecord rec = new SinkRecord("testTopic", 0, null, null, null, msg, 0l);
    rec.headers().addString("my.timestamp", "2020/02/06 16:09:55");

    //init transormer
    TimestampConverter t = new TimestampConverter();
    Map<String,String> tConf = new HashMap<>();
    tConf.put(TimestampConverter.SOURCE_PATTERN_CONFIG, "yyyy/MM/dd HH:mm:ss");
    tConf.put(TimestampConverter.APPLY_TO_CONFIG, "HEADER.my.timestamp");
    tConf.put(TimestampConverter.TARGET_TYPE_CONFIG, "epoch");
    t.configure(tConf);
   
    SinkRecord resultRec = (SinkRecord) t.apply(rec);
    t.close();
    
    Assert.assertNotNull(resultRec.headers().lastWithName("my.timestamp"));
    Assert.assertTrue(resultRec.headers().lastWithName("my.timestamp").value() instanceof  String);
    Assert.assertEquals(Long.valueOf(1581005395000L), Long.valueOf((String) resultRec.headers().lastWithName("my.timestamp").value()));

  }

  
  
  @Test
  public void testSimpleTimestampTransform() {
    String msg = "kkk";
    
   
    SinkRecord rec = new SinkRecord("testTopic", 0, null, null, null, msg, 0l);
    rec.headers().addString("my.timestamp", "2019-11-19T16:15:47.923Z");
    
    //init transormer
    
    TimestampConverter t = new TimestampConverter();
    Map<String,String> tConf = new HashMap<>();
    tConf.put(TimestampConverter.APPLY_TO_CONFIG, "HEADER.my.timestamp");
    tConf.put(TimestampConverter.TARGET_TYPE_CONFIG, "epoch");
    t.configure(tConf);
    
   
    SinkRecord resultRec = (SinkRecord) t.apply(rec);
    t.close();
    
    Assert.assertNotNull(resultRec.headers().lastWithName("my.timestamp"));
    Assert.assertTrue(resultRec.headers().lastWithName("my.timestamp").value() instanceof  String);
    Assert.assertEquals(Long.valueOf(1574180147923L), Long.valueOf((String) resultRec.headers().lastWithName("my.timestamp").value()));

  }
  
  
  @Test
  public void testWrongFormatWithDefaultsTZ() {
    String msg = "kkk";
    
    SinkRecord rec = new SinkRecord("testTopic", 0, null, null, null, msg, 0l);
    rec.headers().addString("my.timestamp", "xxxxxxxxxxxx");

    //init transormer
    TimestampConverter t = new TimestampConverter();
    Map<String,String> tConf = new HashMap<>();
    tConf.put(TimestampConverter.SOURCE_PATTERN_CONFIG, "yyyy/MM/dd HH:mm:ss");
    tConf.put(TimestampConverter.APPLY_TO_CONFIG, "HEADER.my.timestamp");
    tConf.put(TimestampConverter.TARGET_TYPE_CONFIG, "epoch");
    t.configure(tConf);
   
    SinkRecord resultRec;
    try {
      resultRec = (SinkRecord) t.apply(rec);
      Assert.fail();
    } catch (Exception e) {
    }
    
    tConf.put(TimestampConverter.IGNORE_PARSE_ERROR_CONFIG, "true");
    t.configure(tConf);
    resultRec = (SinkRecord) t.apply(rec);
    //does not fail anymore
    Assert.assertNotNull(resultRec.headers().lastWithName("my.timestamp"));
  
    String targetDefault = String.valueOf(Instant.now().toEpochMilli()); 
    tConf.put(TimestampConverter.TARGET_DEFAULT_CONFIG, targetDefault);
    t.configure(tConf);
    resultRec = (SinkRecord) t.apply(rec);
    t.close();
    
    Assert.assertNotNull(resultRec.headers().lastWithName("my.timestamp"));
    Assert.assertTrue(resultRec.headers().lastWithName("my.timestamp").value() instanceof  String);
    Assert.assertEquals(targetDefault, resultRec.headers().lastWithName("my.timestamp").value());
  }
  
  @Test
  public void testWithCondition() {
    String msg = "kkk";
    
   
    SinkRecord rec = new SinkRecord("testTopic", 0, null, null, null, msg, 0l);
    rec.headers().addString("my.timestamp", "2019-11-19T16:15:47.923Z");
    
    
    //init transormer
    TimestampConverter t = new TimestampConverter();
    Map<String,String> tConf = new HashMap<>();
    tConf.put(TimestampConverter.APPLY_TO_CONFIG, "HEADER.my.timestamp");
    tConf.put(TimestampConverter.TARGET_TYPE_CONFIG, "epoch");
    tConf.put(TimestampConverter.CONDITION_CONFIG+"."+IfRegex.IF_CONFIG, "^2019.*");
    t.configure(tConf);
    
   
    SinkRecord resultRec = (SinkRecord) t.apply(rec);
    Assert.assertNotNull(resultRec.headers().lastWithName("my.timestamp"));
    Assert.assertTrue(resultRec.headers().lastWithName("my.timestamp").value() instanceof  String);
    Assert.assertEquals("1574180147923", resultRec.headers().lastWithName("my.timestamp").value());
    
    
    //add non-matching condition
    tConf.put(TimestampConverter.CONDITION_CONFIG+"."+IfRegex.IF_CONFIG, "^XXX.*");
    t.configure(tConf);
    rec = new SinkRecord("testTopic", 0, null, null, null, msg, 0l);
    rec.headers().addString("my.timestamp", "2019-11-19T16:15:47.923Z");
    resultRec = (SinkRecord) t.apply(rec);
    Assert.assertNotNull(resultRec.headers().lastWithName("my.timestamp"));
    Assert.assertEquals("2019-11-19T16:15:47.923Z", resultRec.headers().lastWithName("my.timestamp").value());
    t.close();
    
    
    
    //check condiftion from another header 
    tConf.put(TimestampConverter.CONDITION_CONFIG+"."+IfRegex.IF_CONFIG, "^XXX$");
    tConf.put(TimestampConverter.CONDITION_CONFIG+"."+IfRegex.IF_DATA_CONFIG, "${HEADER.my_type}");
    t.configure(tConf);
    rec = new SinkRecord("testTopic", 0, null, null, null, msg, 0l);
    rec.headers().addString("my.timestamp", "2019-11-19T16:15:47.923Z");
    rec.headers().addString("my_type", "XXX");
    resultRec = (SinkRecord) t.apply(rec);
    Assert.assertNotNull(resultRec.headers().lastWithName("my.timestamp"));
    Assert.assertEquals("1574180147923", resultRec.headers().lastWithName("my.timestamp").value());
    t.close();
    
    
    //check *non-matching* condition from another header 
    tConf.put(TimestampConverter.CONDITION_CONFIG+"."+IfRegex.IF_CONFIG, "^XXX$");
    tConf.put(TimestampConverter.CONDITION_CONFIG+"."+IfRegex.IF_DATA_CONFIG, "${HEADER.my_type}");
    t.configure(tConf);
    rec = new SinkRecord("testTopic", 0, null, null, null, msg, 0l);
    rec.headers().addString("my.timestamp", "2019-11-19T16:15:47.923Z");
    rec.headers().addString("my_type", "XXXXXXX");
    resultRec = (SinkRecord) t.apply(rec);
    Assert.assertNotNull(resultRec.headers().lastWithName("my.timestamp"));
    Assert.assertEquals("2019-11-19T16:15:47.923Z", resultRec.headers().lastWithName("my.timestamp").value());
    t.close();
    
    
    //check condiftion from VALUE
    tConf.put(TimestampConverter.CONDITION_CONFIG+"."+IfRegex.IF_CONFIG, "k+");
    tConf.put(TimestampConverter.CONDITION_CONFIG+"."+IfRegex.IF_DATA_CONFIG, "${VALUE}");
    t.configure(tConf);
    rec = new SinkRecord("testTopic", 0, null, null, null, msg, 0l);
    rec.headers().addString("my.timestamp", "2019-11-19T16:15:47.923Z");
    rec.headers().addString("my_type", "XXX");
    resultRec = (SinkRecord) t.apply(rec);
    Assert.assertNotNull(resultRec.headers().lastWithName("my.timestamp"));
    Assert.assertEquals("1574180147923", resultRec.headers().lastWithName("my.timestamp").value());
    t.close();
    
    
    
    //check *non-matching* condition from another header 
    tConf.put(TimestampConverter.CONDITION_CONFIG+"."+IfRegex.IF_CONFIG, "^XXX$");
    tConf.put(TimestampConverter.CONDITION_CONFIG+"."+IfRegex.IF_DATA_CONFIG, "${HEADER.non_existing}");
    t.configure(tConf);
    rec = new SinkRecord("testTopic", 0, null, null, null, msg, 0l);
    rec.headers().addString("my.timestamp", "2019-11-19T16:15:47.923Z");
    rec.headers().addString("my_type", "XXXXXXX");
    resultRec = (SinkRecord) t.apply(rec);
    Assert.assertNotNull(resultRec.headers().lastWithName("my.timestamp"));
    Assert.assertEquals("2019-11-19T16:15:47.923Z", resultRec.headers().lastWithName("my.timestamp").value());
    t.close();

    

    

  }
  

  
}
