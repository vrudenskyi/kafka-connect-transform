package com.vrudenskyi.kafka.connect.transform;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ToJsonUnsupportedSchemaTypesTest {

    @Parameters(name = "{index}: schema={0}, value={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {Schema.INT8_SCHEMA, (byte) 1, "Only supported schema types can be converted to JSON; specified [INT8], supported [ARRAY, MAP, STRUCT]"},
                {Schema.INT16_SCHEMA, (short) 2, "Only supported schema types can be converted to JSON; specified [INT16], supported [ARRAY, MAP, STRUCT]"},
                {Schema.INT32_SCHEMA, 3, "Only supported schema types can be converted to JSON; specified [INT32], supported [ARRAY, MAP, STRUCT]"},
                {Schema.INT64_SCHEMA, 4L, "Only supported schema types can be converted to JSON; specified [INT64], supported [ARRAY, MAP, STRUCT]"},
                {Schema.FLOAT32_SCHEMA, 5.6F, "Only supported schema types can be converted to JSON; specified [FLOAT32], supported [ARRAY, MAP, STRUCT]"},
                {Schema.FLOAT64_SCHEMA, 6.7D, "Only supported schema types can be converted to JSON; specified [FLOAT64], supported [ARRAY, MAP, STRUCT]"},
                {Schema.BOOLEAN_SCHEMA, true, "Only supported schema types can be converted to JSON; specified [BOOLEAN], supported [ARRAY, MAP, STRUCT]"},
                {Schema.STRING_SCHEMA, "Hello World", "Only supported schema types can be converted to JSON; specified [STRING], supported [ARRAY, MAP, STRUCT]"},
                {Schema.BYTES_SCHEMA, new byte[]{1, 2, 3}, "Only supported schema types can be converted to JSON; specified [BYTES], supported [ARRAY, MAP, STRUCT]"},
                {Decimal.schema(3), new BigDecimal("123.456"), "Only supported schema types can be converted to JSON; specified [BYTES-org.apache.kafka.connect.data.Decimal], supported [ARRAY, MAP, STRUCT]"},
                {Date.SCHEMA, new java.util.Date(999L), "Only supported schema types can be converted to JSON; specified [INT32-org.apache.kafka.connect.data.Date], supported [ARRAY, MAP, STRUCT]"},
                {Time.SCHEMA, new java.util.Date(999L), "Only supported schema types can be converted to JSON; specified [INT32-org.apache.kafka.connect.data.Time], supported [ARRAY, MAP, STRUCT]"},
                {Timestamp.SCHEMA, new java.util.Date(999L), "Only supported schema types can be converted to JSON; specified [INT64-org.apache.kafka.connect.data.Timestamp], supported [ARRAY, MAP, STRUCT]"}
        });
    }

    private final Schema schema;
    private final Object value;
    private final String expectedErrorMessage;

    public ToJsonUnsupportedSchemaTypesTest(Schema schema, Object value, String expectedErrorMessage) {
        this.schema = schema;
        this.value = value;
        this.expectedErrorMessage = expectedErrorMessage;
    }

    private ToJson<SourceRecord> sut;

    @Before
    public void setup() {
        sut = new ToJson<>();
    }


    @Test
    public void shouldThrowExceptionForUnsupportedSchema() {
        // given
        SourceRecord sourceRecord = new SourceRecord(null, null, "testTopic", schema, value);

        // when
        ConnectException connectException = assertThrows(ConnectException.class, () -> sut.apply(sourceRecord));

        // then
        assertEquals(expectedErrorMessage, connectException.getMessage());
    }
}
