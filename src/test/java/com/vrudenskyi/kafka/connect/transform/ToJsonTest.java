package com.vrudenskyi.kafka.connect.transform;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;

import static org.easymock.EasyMock.createMock;
import static org.junit.Assert.assertNotNull;

public class ToJsonTest {
    private static final String LOGICAL_FIELD_NAME = "logicalField";

    private ToJson<SourceRecord> sut;

    @Before
    public void setup() {
        sut = new ToJson<>();
    }

    @Test
    public void shouldConfigure() {
        // given
        // when
        sut.configure(new HashMap<>());

        // then
        assertNotNull(sut);
    }

    @Test
    public void shouldGetConfig() {
        // given
        // when
        ConfigDef config = sut.config();

        // then
        assertNotNull(config);
    }

    @Test
    public void shouldClose() {
        // given
        // when
        sut.close();

        // then
        assertNotNull(sut);
    }

    @Test
    public void shouldNotConvertNullValue() {
        // given
        SourceRecord sourceRecord = getRecord(Schema.OPTIONAL_STRING_SCHEMA, null);

        // when
        SourceRecord transformedRecord = sut.apply(sourceRecord);

        // then
        Assert.assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema());
        Assert.assertNull(transformedRecord.value());
    }

    @Test
    public void shouldConvertStructWithAllCorePrimitiveSchemaTypesToJson() {
        // given
        Schema valueSchema = SchemaBuilder.struct()
                .field("INT8-field", Schema.INT8_SCHEMA)
                .field("INT16-field", Schema.INT16_SCHEMA)
                .field("INT32-field", Schema.INT32_SCHEMA)
                .field("INT64-field", Schema.INT64_SCHEMA)
                .field("FLOAT32-field", Schema.FLOAT32_SCHEMA)
                .field("FLOAT64-field", Schema.FLOAT64_SCHEMA)
                .field("BOOLEAN-field", Schema.BOOLEAN_SCHEMA)
                .field("STRING-field", Schema.STRING_SCHEMA)
                .field("BYTES-field", Schema.BYTES_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema);
        value.put("INT8-field", (byte) 1);
        value.put("INT16-field", (short) 2);
        value.put("INT32-field", 3);
        value.put("INT64-field", 4L);
        value.put("FLOAT32-field", 5.6F);
        value.put("FLOAT64-field", 6.7D);
        value.put("BOOLEAN-field", true);
        value.put("STRING-field", "Hello World");
        value.put("BYTES-field", new byte[]{1, 2, 3});

        SourceRecord sourceRecord = getRecord(valueSchema, value);

        // when
        SourceRecord transformedRecord = sut.apply(sourceRecord);

        // then
        Assert.assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema());
        Assert.assertEquals("{\"INT8-field\":1,\"INT16-field\":2,\"INT32-field\":3,\"INT64-field\":4,\"FLOAT32-field\":5.6,\"FLOAT64-field\":6.7,\"BOOLEAN-field\":true,\"STRING-field\":\"Hello World\",\"BYTES-field\":\"AQID\"}", transformedRecord.value().toString());
    }

    @Test
    public void shouldConvertArrayOfStringSchemaTypeToJson() {
        // given
        Schema valueSchema = SchemaBuilder.array(Schema.STRING_SCHEMA)
                .build();

        ArrayList<String> value = new ArrayList<>();
        value.add("list-String-1");

        SourceRecord sourceRecord = getRecord(valueSchema, value);

        // when
        SourceRecord transformedRecord = sut.apply(sourceRecord);

        // then
        Assert.assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema());
        Assert.assertEquals("[\"list-String-1\"]", transformedRecord.value().toString());
    }

    @Test
    public void shouldConvertMapOfStringStringSchemaTypeToJson() {
        // given
        Schema valueSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA)
                .build();

        HashMap<Object, Object> value = new HashMap<>();
        value.put("map-String-key", "map-String-value");

        SourceRecord sourceRecord = getRecord(valueSchema, value);

        // when
        SourceRecord transformedRecord = sut.apply(sourceRecord);

        // then
        Assert.assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema());
        Assert.assertEquals("{\"map-String-key\":\"map-String-value\"}", transformedRecord.value().toString());
    }

    @Test
    public void shouldConvertTwoLevelNestedStructValueToJson() {
        // given
        Schema structArrayStructSchema = SchemaBuilder.struct()
                .field("structArrayStructStringField", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        Schema structFieldSchema = SchemaBuilder.struct()
                .field("stringSubField", Schema.STRING_SCHEMA)
                .field("stringArraySubField", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("structArraySubField", SchemaBuilder.array(structArrayStructSchema))
                .build();

        Schema valueSchema = SchemaBuilder.struct()
                .field("stringField", Schema.STRING_SCHEMA)
                .field("structField", structFieldSchema)
                .build();

        Struct value = getTwoLevelNestedStructWithArrayFields(valueSchema, structFieldSchema, structArrayStructSchema);
        SourceRecord sourceRecord = getRecord(valueSchema, value);

        // when
        SourceRecord transformedRecord = sut.apply(sourceRecord);

        // then
        Assert.assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema());
        Assert.assertEquals("{\"stringField\":\"Hello from Top o' the World\",\"structField\":{\"stringSubField\":\"Hello from the Underworld\",\"stringArraySubField\":[\"foo\",\"bar\"],\"structArraySubField\":[{\"structArrayStructStringField\":\"structArrayStructStringField-1\"},{\"structArrayStructStringField\":\"structArrayStructStringField-2\"}]}}", transformedRecord.value().toString());
    }

    private static Struct getTwoLevelNestedStructWithArrayFields(Schema valueSchema, Schema structFieldSchema, Schema structArraySchema) {
        Struct value = new Struct(valueSchema);
        value.put("stringField", "Hello from Top o' the World");

        Struct structFieldValue = new Struct(structFieldSchema);
        structFieldValue.put("stringSubField", "Hello from the Underworld");

        ArrayList<String> stringArray = new ArrayList<>();
        stringArray.add("foo");
        stringArray.add("bar");
        structFieldValue.put("stringArraySubField", stringArray);

        ArrayList<Struct> structArray = new ArrayList<>();
        structArray.add(new Struct(structArraySchema).put("structArrayStructStringField", "structArrayStructStringField-1"));
        structArray.add(new Struct(structArraySchema).put("structArrayStructStringField", "structArrayStructStringField-2"));
        structFieldValue.put("structArraySubField", structArray);

        value.put("structField", structFieldValue);

        return value;
    }

    @Test
    public void shouldConvertStructWithLogicalDecimalTypeToJson() {
        // given
        Schema logicalTypeSchema = Decimal.schema(3);
        Schema valueSchema = SchemaBuilder.struct()
                .field(LOGICAL_FIELD_NAME, logicalTypeSchema)
                .build();

        Struct value = new Struct(valueSchema);
        value.put(LOGICAL_FIELD_NAME, new BigDecimal("123.456"));

        SourceRecord sourceRecord = getRecord(valueSchema, value);

        // when
        SourceRecord transformedRecord = sut.apply(sourceRecord);

        // then
        Assert.assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema());
        Assert.assertEquals("{\"logicalField\":123.456}", transformedRecord.value().toString());
    }

    @Test
    public void shouldConvertStructWithLogicalDateTypeToJson() {
        // given
        Schema logicalTypeSchema = Date.SCHEMA;
        Schema valueSchema = SchemaBuilder.struct()
                .field(LOGICAL_FIELD_NAME, logicalTypeSchema)
                .build();

        Struct value = new Struct(valueSchema);
        value.put(LOGICAL_FIELD_NAME, new java.util.Date(999L));

        SourceRecord sourceRecord = getRecord(valueSchema, value);

        // when
        SourceRecord transformedRecord = sut.apply(sourceRecord);

        // then
        Assert.assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema());
        Assert.assertEquals("{\"logicalField\":\"1970-01-01T00:00:00.999+0000\"}", transformedRecord.value().toString());
    }

    @Test
    public void shouldConvertStructWithLogicalTimeTypeToJson() {
        // given
        Schema logicalTypeSchema = Time.SCHEMA;
        Schema valueSchema = SchemaBuilder.struct()
                .field(LOGICAL_FIELD_NAME, logicalTypeSchema)
                .build();

        Struct value = new Struct(valueSchema);
        value.put(LOGICAL_FIELD_NAME, new java.util.Date(999L));

        SourceRecord sourceRecord = getRecord(valueSchema, value);

        // when
        SourceRecord transformedRecord = sut.apply(sourceRecord);

        // then
        Assert.assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema());
        Assert.assertEquals("{\"logicalField\":\"1970-01-01T00:00:00.999+0000\"}", transformedRecord.value().toString());
    }

    @Test
    public void shouldConvertStructWithLogicalTimestampTypeToJson() {
        // given
        Schema logicalTypeSchema = Timestamp.SCHEMA;
        Schema valueSchema = SchemaBuilder.struct()
                .field(LOGICAL_FIELD_NAME, logicalTypeSchema)
                .build();

        Struct value = new Struct(valueSchema);
        value.put(LOGICAL_FIELD_NAME, new java.util.Date(999L));

        SourceRecord sourceRecord = getRecord(valueSchema, value);

        // when
        SourceRecord transformedRecord = sut.apply(sourceRecord);

        // then
        Assert.assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema());
        Assert.assertEquals("{\"logicalField\":\"1970-01-01T00:00:00.999+0000\"}", transformedRecord.value().toString());
    }

    @Test
    public void shouldCatchJsonProcessingExceptionAndSetErrorHeader() {
        // given
        Struct mockStruct = createMock(Struct.class);
        SourceRecord sourceRecord = getRecord(SchemaBuilder.struct().build(), mockStruct);

        // when
        SourceRecord transformedRecord = sut.apply(sourceRecord);

        // then
        Header errorHeader = transformedRecord.headers().lastWithName("error");
        Assert.assertEquals("[no message for java.lang.NullPointerException]", errorHeader.value());
        Assert.assertEquals(sourceRecord, transformedRecord);
        Assert.assertEquals(mockStruct, transformedRecord.value());
    }

    private SourceRecord getRecord(Schema valueSchema, Object value) {
        return new SourceRecord(null, null, "testTopic", valueSchema, value);
    }
}
