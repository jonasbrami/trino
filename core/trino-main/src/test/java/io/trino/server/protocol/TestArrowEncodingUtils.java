/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server.protocol;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.client.CloseableIterator;
import io.trino.client.Column;
import io.trino.client.QueryDataDecoder;
import io.trino.client.Row;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.encoding.ArrowQueryDataDecoder;
import io.trino.client.spooling.encoding.JsonQueryDataDecoder;
import io.trino.json.JsonItems;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.encoding.ArrowCompressionFactory;
import io.trino.server.protocol.spooling.encoding.ArrowQueryDataEncoder;
import io.trino.server.protocol.spooling.encoding.JsonQueryDataEncoder;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.IntervalYearMonthType;
import io.trino.type.JsonType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createCharsBlock;
import static io.trino.block.BlockAssertions.createDateSequenceBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createSmallintsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.block.BlockAssertions.createTimestampSequenceBlock;
import static io.trino.block.BlockAssertions.createTinyintsBlock;
import static io.trino.block.BlockAssertions.createTypedLongsBlock;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.server.protocol.ProtocolUtil.createColumn;
import static io.trino.server.protocol.spooling.encoding.arrow.TrinoToArrowTypeConverter.toArrowField;
import static io.trino.server.protocol.spooling.encoding.arrow.VectorWriters.writerForVector;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.IntervalField.DAY;
import static io.trino.spi.type.IntervalField.HOUR;
import static io.trino.spi.type.IntervalField.SECOND;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.TIME_NANOS;
import static io.trino.spi.type.TimeType.TIME_PICOS;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MICROS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_NANOS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_SECONDS;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_PICOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static io.trino.type.IntervalDayTimeType.createIntervalDayTimeType;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestArrowEncodingUtils
{
    private BufferAllocator allocator;

    @BeforeEach
    public void setup()
    {
        allocator = new RootAllocator();
    }

    @AfterEach
    public void tearDown()
    {
        allocator.close();
    }

    protected QueryDataDecoder createDecoder(List<Column> columns)
    {
        return new ArrowQueryDataDecoder.Factory().create(columns, DataAttributes.empty(), false);
    }

    protected QueryDataEncoder createEncoder(List<OutputColumn> columns)
    {
        return new ArrowQueryDataEncoder.Factory(allocator, Optional.empty()).create(TEST_SESSION, columns);
    }

    @Test
    public void testBigintSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", BIGINT));
        Page page = page(createTypedLongsBlock(BIGINT, 1L, 2L, 3L, 4L));

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).containsExactly(
                List.of(1L),
                List.of(2L),
                List.of(3L),
                List.of(4L));
    }

    @Test
    public void testIntegerSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", INTEGER));
        Page page = page(createIntsBlock(1, 2, 3, 4));

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).containsExactly(
                List.of(1),
                List.of(2),
                List.of(3),
                List.of(4));
    }

    @Test
    public void testSmallintSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", SMALLINT));
        Page page = page(createSmallintsBlock(1, 2, 3, 4));

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).containsExactly(
                List.of((short) 1),
                List.of((short) 2),
                List.of((short) 3),
                List.of((short) 4));
    }

    @Test
    public void testTinyintSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TINYINT));
        Page page = page(createTinyintsBlock(1, 2, 3, 4));

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).containsExactly(
                List.of((byte) 1),
                List.of((byte) 2),
                List.of((byte) 3),
                List.of((byte) 4));
    }

    @Test
    public void testDoubleSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", DOUBLE));
        Page page = page(createDoublesBlock(1.0, 2.11, 3.11, 4.13));

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).containsExactly(
                List.of(1.0),
                List.of(2.11),
                List.of(3.11),
                List.of(4.13));
    }

    @Test
    public void testRealSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", REAL));
        Page page = page(createBlockOfReals(1.0f, 2.11f, 3.11f, 4.13f));

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).containsExactly(
                List.of(1.0f),
                List.of(2.11f),
                List.of(3.11f),
                List.of(4.13f));
    }

    @Test
    public void testBooleanSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", BOOLEAN));
        Page page = page(createBooleansBlock(true, false, true, false));

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).containsExactly(
                List.of(true),
                List.of(false),
                List.of(true),
                List.of(false));
    }

    @Test
    public void testVarcharSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", VARCHAR));
        Page page = page(createStringsBlock("hello", "world", "arrow", "test"));

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).containsExactly(
                List.of("hello"),
                List.of("world"),
                List.of("arrow"),
                List.of("test"));
    }

    @Test
    public void testCharSerialization()
            throws IOException
    {
        CharType charType = createCharType(5);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", charType));
        Page page = page(createCharsBlock(charType, List.of("hello", "world", "test")));

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).containsExactly(
                List.of("hello"),
                List.of("world"),
                List.of("test "));
    }

    @Test
    public void testVarbinarySerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", VARBINARY));
        BlockBuilder blockBuilder = VARBINARY.createBlockBuilder(null, 3);
        VARBINARY.writeSlice(blockBuilder, utf8Slice("hello"));
        VARBINARY.writeSlice(blockBuilder, utf8Slice("world"));
        VARBINARY.writeSlice(blockBuilder, utf8Slice("test"));
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(3);
        // Varbinary values are returned as byte arrays
    }

    @Test
    public void testDateSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", DATE));
        Page page = page(createDateSequenceBlock(0, 3));

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(3);
        // Dates are represented as days since epoch
    }

    @Test
    public void testDecimalSerialization()
            throws IOException
    {
        // Use short decimal (precision <= 18) to avoid Int128ArrayBlock issues
        List<TypedColumn> columns = ImmutableList.of(typed("col0", createDecimalType(10, 3)));
        BlockBuilder blockBuilder = createDecimalType(10, 3).createBlockBuilder(null, 3);
        createDecimalType(10, 3).writeLong(blockBuilder, 123456L); // 123.456
        createDecimalType(10, 3).writeLong(blockBuilder, 789123L); // 789.123
        createDecimalType(10, 3).writeLong(blockBuilder, 456789L); // 456.789
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(3);
    }

    @Test
    public void testTimeMillisSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIME_MILLIS));
        BlockBuilder blockBuilder = TIME_MILLIS.createBlockBuilder(null, 2);
        TIME_MILLIS.writeLong(blockBuilder, 12345000L); // microseconds since midnight
        TIME_MILLIS.writeLong(blockBuilder, 67890000L);
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTimeMicrosSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIME_MICROS));
        BlockBuilder blockBuilder = TIME_MICROS.createBlockBuilder(null, 2);
        TIME_MICROS.writeLong(blockBuilder, 12345000000L); // picoseconds since midnight
        TIME_MICROS.writeLong(blockBuilder, 67890000000L);
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTimeNanosSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIME_NANOS));
        BlockBuilder blockBuilder = TIME_NANOS.createBlockBuilder(null, 2);
        TIME_NANOS.writeLong(blockBuilder, 12345000000000L); // picoseconds since midnight
        TIME_NANOS.writeLong(blockBuilder, 67890000000000L);
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTimeSecondsSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIME_SECONDS));
        BlockBuilder blockBuilder = TIME_SECONDS.createBlockBuilder(null, 2);

        // Test seconds precision values that result in meaningful times
        // 14:30:25 = (14*3600 + 30*60 + 25) seconds = 52225 seconds = 52225000000000000 picoseconds
        TIME_SECONDS.writeLong(blockBuilder, 52225000000000000L); // 14:30:25
        // 09:15:45 = (9*3600 + 15*60 + 45) seconds = 33345 seconds = 33345000000000000 picoseconds
        TIME_SECONDS.writeLong(blockBuilder, 33345000000000000L); // 09:15:45

        Page page = page(blockBuilder.build());
        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).containsExactly(
                List.of("14:30:25"),
                List.of("09:15:45"));
    }

    @Test
    public void testTimePicosSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIME_PICOS));
        BlockBuilder blockBuilder = TIME_PICOS.createBlockBuilder(null, 2);

        // Test picosecond precision values that result in meaningful times
        // 14:30:25.123456789 = (14*3600 + 30*60 + 25) seconds + 123456789 nanoseconds
        // = 52225 seconds = 52225000000000000 picoseconds + 123456789000 picoseconds = 52225123456789000L
        TIME_PICOS.writeLong(blockBuilder, 52225123456789000L); // 14:30:25.123456789 -> truncated to 14:30:25.123456789
        // 09:15:45.987654321 = (9*3600 + 15*60 + 45) seconds + 987654321 nanoseconds
        // = 33345 seconds = 33345000000000000 picoseconds + 987654321000 picoseconds = 33345987654321000L
        TIME_PICOS.writeLong(blockBuilder, 33345987654321000L); // 09:15:45.987654321 -> truncated to 09:15:45.987654321

        Page page = page(blockBuilder.build());
        List<List<Object>> result = roundTrip(columns, page);

        // Arrow supports nanosecond precision max, so picoseconds get truncated to nanoseconds
        // The decoder returns strings for TIME types
        assertThat(result).containsExactly(
                List.of("14:30:25.123456789"), // 52225123456789000 picoseconds -> 52225123456789 nanoseconds -> 14:30:25.123456789
                List.of("09:15:45.987654321")); // 33345987654321000 picoseconds -> 33345987654321 nanoseconds -> 09:15:45.987654321
    }

    @Test
    public void testTimeWithTimeZoneMillisSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIME_TZ_MILLIS));
        BlockBuilder blockBuilder = TIME_TZ_MILLIS.createBlockBuilder(null, 2);
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(12345000L, 0)); // packed value
        TIME_TZ_MILLIS.writeLong(blockBuilder, packTimeWithTimeZone(67890000L, 60));
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTimeWithTimeZoneSecondsSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIME_TZ_SECONDS));
        BlockBuilder blockBuilder = TIME_TZ_SECONDS.createBlockBuilder(null, 2);
        TIME_TZ_SECONDS.writeLong(blockBuilder, packTimeWithTimeZone(12345L, 0)); // packed value
        TIME_TZ_SECONDS.writeLong(blockBuilder, packTimeWithTimeZone(67890L, 60));
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTimeWithTimeZoneMicrosSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIME_TZ_MICROS));
        BlockBuilder blockBuilder = TIME_TZ_MICROS.createBlockBuilder(null, 2);
        TIME_TZ_MICROS.writeLong(blockBuilder, packTimeWithTimeZone(12345000000L, 0)); // packed value
        TIME_TZ_MICROS.writeLong(blockBuilder, packTimeWithTimeZone(67890000000L, 60));
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTimeWithTimeZoneNanosSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIME_TZ_NANOS));
        BlockBuilder blockBuilder = TIME_TZ_NANOS.createBlockBuilder(null, 2);
        TIME_TZ_NANOS.writeLong(blockBuilder, packTimeWithTimeZone(12345000000000L, 0)); // packed value
        TIME_TZ_NANOS.writeLong(blockBuilder, packTimeWithTimeZone(67890000000000L, 60));
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTimestampMillisSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIMESTAMP_MILLIS));
        Page page = page(createTimestampSequenceBlock(0, 3));

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(3);
    }

    @Test
    public void testTimestampMicrosSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIMESTAMP_MICROS));
        BlockBuilder blockBuilder = TIMESTAMP_MICROS.createBlockBuilder(null, 2);
        TIMESTAMP_MICROS.writeLong(blockBuilder, 1234567890123456L);
        TIMESTAMP_MICROS.writeLong(blockBuilder, 9876543210987654L);
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTimestampNanosSerialization()
            throws IOException
    {
        // Nanosecond timestamps require special handling with LongTimestamp
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIMESTAMP_NANOS));
        BlockBuilder blockBuilder = TIMESTAMP_NANOS.createBlockBuilder(null, 2);
        // Write LongTimestamp objects (epochMicros, picosOfMicro)
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(1234567890123L, 456_000)); // 456 nanoseconds
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(9876543210987L, 789_000)); // 789 nanoseconds
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTimestampSecondsSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIMESTAMP_SECONDS));
        BlockBuilder blockBuilder = TIMESTAMP_SECONDS.createBlockBuilder(null, 2);
        TIMESTAMP_SECONDS.writeLong(blockBuilder, 1234567890L);
        TIMESTAMP_SECONDS.writeLong(blockBuilder, 9876543210L);
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTimestampWithTimeZoneMillisSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIMESTAMP_TZ_MILLIS));
        BlockBuilder blockBuilder = TIMESTAMP_TZ_MILLIS.createBlockBuilder(null, 2);
        TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(1234567890123L, 0));
        TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packDateTimeWithZone(9876543210987L, 60));
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTimestampWithTimeZoneNanosSerialization()
            throws IOException
    {
        // Nanosecond timestamps with timezone require LongTimestampWithTimeZone objects
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIMESTAMP_TZ_NANOS));
        BlockBuilder blockBuilder = TIMESTAMP_TZ_NANOS.createBlockBuilder(null, 2);

        // Create LongTimestampWithTimeZone objects (epochMillis, picosOfMilli, timeZoneKey)
        // UTC timezone (key = 0)
        LongTimestampWithTimeZone timestamp1 = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                1234567890123L, 456_000, (short) 0); // 456,000 picoseconds = 456 nanoseconds within the millisecond, UTC

        // +1 hour timezone (key = 60 minutes offset)
        LongTimestampWithTimeZone timestamp2 = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                9876543210987L, 789_000, (short) 60); // 789,000 picoseconds = 789 nanoseconds within the millisecond, +1 hour

        TIMESTAMP_TZ_NANOS.writeObject(blockBuilder, timestamp1);
        TIMESTAMP_TZ_NANOS.writeObject(blockBuilder, timestamp2);
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTimestampWithTimeZoneSecondsSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIMESTAMP_TZ_SECONDS));
        BlockBuilder blockBuilder = TIMESTAMP_TZ_SECONDS.createBlockBuilder(null, 2);
        TIMESTAMP_TZ_SECONDS.writeLong(blockBuilder, packDateTimeWithZone(1234567890L, 0));
        TIMESTAMP_TZ_SECONDS.writeLong(blockBuilder, packDateTimeWithZone(9876543210L, 60));
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTimestampPicosSerialization()
            throws IOException
    {
        // Picosecond timestamps should be automatically cast to nanosecond precision for Arrow compatibility
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIMESTAMP_PICOS));
        BlockBuilder blockBuilder = TIMESTAMP_PICOS.createBlockBuilder(null, 2);

        // Create LongTimestamp objects with picosecond precision
        LongTimestamp timestamp1 = new LongTimestamp(1234567890123456L, 789123); // 789,123 picoseconds
        LongTimestamp timestamp2 = new LongTimestamp(9876543210987654L, 456789); // 456,789 picoseconds

        TIMESTAMP_PICOS.writeObject(blockBuilder, timestamp1);
        TIMESTAMP_PICOS.writeObject(blockBuilder, timestamp2);
        Page page = page(blockBuilder.build());

        // Should successfully round-trip, with picoseconds truncated to nanoseconds
        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
        // Note: Precision loss expected - picoseconds are truncated to nanoseconds
    }

    @Test
    public void testTimestampWithTimeZonePicosSerialization()
            throws IOException
    {
        // Picosecond timestamps with timezone should be automatically cast to nanosecond precision for Arrow compatibility
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIMESTAMP_TZ_PICOS));
        BlockBuilder blockBuilder = TIMESTAMP_TZ_PICOS.createBlockBuilder(null, 2);

        // Create LongTimestampWithTimeZone objects with picosecond precision
        LongTimestampWithTimeZone timestamp1 = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                1234567890123L, 789123, (short) 0); // 789,123 picoseconds, UTC
        LongTimestampWithTimeZone timestamp2 = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                9876543210987L, 456789, (short) 60); // 456,789 picoseconds, +1 hour

        TIMESTAMP_TZ_PICOS.writeObject(blockBuilder, timestamp1);
        TIMESTAMP_TZ_PICOS.writeObject(blockBuilder, timestamp2);
        Page page = page(blockBuilder.build());

        // Should successfully round-trip, with picoseconds truncated to nanoseconds
        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
        // Note: Precision loss expected - picoseconds are truncated to nanoseconds
    }

    @Test
    public void testTimestampWithTimeZoneMicrosSerialization()
            throws IOException
    {
        // Microsecond timestamps with timezone require LongTimestampWithTimeZone objects (precision 6 > MAX_SHORT_PRECISION 3)
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIMESTAMP_TZ_MICROS));
        BlockBuilder blockBuilder = TIMESTAMP_TZ_MICROS.createBlockBuilder(null, 2);

        // Create LongTimestampWithTimeZone objects (epochMillis, picosOfMilli, timeZoneKey)
        LongTimestampWithTimeZone timestamp1 = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                1234567890123L, 456_000, (short) 0); // UTC
        LongTimestampWithTimeZone timestamp2 = LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                9876543210987L, 654_000, (short) 60); // +1 hour

        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, timestamp1);
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, timestamp2);
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testTimestampOutsideOfNanosecondRange()
            throws IOException
    {
        // Values outside of the range of nanoseconds since the epoch (1677-09-21 to 2262-04-11) must not overflow
        BlockBuilder blockBuilder = TIMESTAMP_MICROS.createBlockBuilder(null, 6);
        TIMESTAMP_MICROS.writeLong(blockBuilder, -62_135_596_800_000_000L); // 0001-01-01 00:00:00
        TIMESTAMP_MICROS.writeLong(blockBuilder, -11_676_096_000_000_000L + 123_456); // 1600-01-01 00:00:00.123456
        TIMESTAMP_MICROS.writeLong(blockBuilder, -1); // 1969-12-31 23:59:59.999999
        TIMESTAMP_MICROS.writeLong(blockBuilder, 9_224_000_000_000_000L); // 2262-04-19 06:13:20
        TIMESTAMP_MICROS.writeLong(blockBuilder, 253_402_300_799_999_999L); // 9999-12-31 23:59:59.999999
        blockBuilder.appendNull();
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIMESTAMP_MICROS));
        Page page = page(blockBuilder.build());

        assertThat(roundTrip(columns, page))
                .containsExactly(
                        List.of("0001-01-01 00:00:00.000000"),
                        List.of("1600-01-01 00:00:00.123456"),
                        List.of("1969-12-31 23:59:59.999999"),
                        List.of("2262-04-19 06:13:20.000000"),
                        List.of("9999-12-31 23:59:59.999999"),
                        Arrays.asList((Object) null))
                .isEqualTo(jsonRoundTrip(columns, page));
    }

    @Test
    public void testTimestampWithTimeZoneOutsideOfNanosecondRange()
            throws IOException
    {
        BlockBuilder blockBuilder = TIMESTAMP_TZ_MICROS.createBlockBuilder(null, 4);
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(-62_135_596_800_000L, 0, UTC_KEY));
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(-11_676_096_000_000L + 123, 456_000_000, UTC_KEY));
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(-1, 999_000_000, UTC_KEY));
        TIMESTAMP_TZ_MICROS.writeObject(blockBuilder, LongTimestampWithTimeZone.fromEpochMillisAndFraction(253_402_300_799_999L, 999_000_000, UTC_KEY));
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIMESTAMP_TZ_MICROS));
        Page page = page(blockBuilder.build());

        assertThat(roundTrip(columns, page))
                .containsExactly(
                        List.of("0001-01-01 00:00:00.000000 UTC"),
                        List.of("1600-01-01 00:00:00.123456 UTC"),
                        List.of("1969-12-31 23:59:59.999999 UTC"),
                        List.of("9999-12-31 23:59:59.999999 UTC"))
                .isEqualTo(jsonRoundTrip(columns, page));
    }

    @Test
    public void testTimestampNanosRange()
            throws IOException
    {
        BlockBuilder blockBuilder = TIMESTAMP_NANOS.createBlockBuilder(null, 3);
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(-9_223_372_036_854_776L, 192_000)); // smallest nanosecond value
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(-1, 1_000)); // 1969-12-31 23:59:59.999999001
        TIMESTAMP_NANOS.writeObject(blockBuilder, new LongTimestamp(9_223_372_036_854_775L, 807_000)); // largest nanosecond value
        List<TypedColumn> columns = ImmutableList.of(typed("col0", TIMESTAMP_NANOS));
        Page page = page(blockBuilder.build());

        assertThat(roundTrip(columns, page))
                .containsExactly(
                        List.of("1677-09-21 00:12:43.145224192"),
                        List.of("1969-12-31 23:59:59.999999001"),
                        List.of("2262-04-11 23:47:16.854775807"))
                .isEqualTo(jsonRoundTrip(columns, page));
    }

    @Test
    public void testDateTimeTextMatchesJsonEncoding()
            throws IOException
    {
        // The Arrow decoder renders values exactly like the JSON encoding, the time zone of values in UTC included
        long[] picosOfDay = {0, 3_723_400_000_000_000L, 45_296_123_456_789_000L, 86_399_999_999_999_000L};
        long[] epochMicros = {0, -1, 1_234_567_890_123_456L, -62_135_596_800_000_000L, 253_402_300_799_999_999L};
        // Nanosecond values are limited to the range of nanoseconds since the epoch
        long[] nanosecondEpochMicros = {0, -1, 1_234_567_890_123_456L, -9_223_372_036_000_000L, 9_223_372_036_854_775L};
        ImmutableList.Builder<TypedColumn> columns = ImmutableList.builder();
        ImmutableList.Builder<Block> blocks = ImmutableList.builder();
        for (int precision : new int[] {0, 3, 6, 9}) {
            long picosFactor = (long) Math.pow(10, 12 - precision);

            TimeType timeType = createTimeType(precision);
            BlockBuilder time = timeType.createBlockBuilder(null, 5);
            TimeWithTimeZoneType timeWithTimeZoneType = createTimeWithTimeZoneType(precision);
            BlockBuilder timeWithTimeZone = timeWithTimeZoneType.createBlockBuilder(null, 5);
            for (int i = 0; i < 5; i++) {
                long picos = picosOfDay[i % picosOfDay.length] / picosFactor * picosFactor;
                timeType.writeLong(time, picos);
                timeWithTimeZoneType.writeLong(timeWithTimeZone, DateTimeEncoding.packTimeWithTimeZone(picos / 1000, 0));
            }

            TimestampType timestampType = createTimestampType(precision);
            BlockBuilder timestamp = timestampType.createBlockBuilder(null, 5);
            TimestampWithTimeZoneType timestampWithTimeZoneType = createTimestampWithTimeZoneType(precision);
            BlockBuilder timestampWithTimeZone = timestampWithTimeZoneType.createBlockBuilder(null, 5);
            for (int i = 0; i < 5; i++) {
                long micros = epochMicros[i];
                // Nanosecond values get sub-microsecond digits, and values are truncated to the precision
                int picosOfMicro = 0;
                if (precision > 6) {
                    micros = nanosecondEpochMicros[i];
                    picosOfMicro = i * 123_000;
                }
                else {
                    long microsFactor = picosFactor / 1_000_000L;
                    micros = Math.floorDiv(micros, microsFactor) * microsFactor;
                }
                if (timestampType.isShort()) {
                    timestampType.writeLong(timestamp, micros);
                }
                else {
                    timestampType.writeObject(timestamp, new LongTimestamp(micros, picosOfMicro));
                }
                long millis = Math.floorDiv(micros, 1000L);
                int picosOfMilli = (int) (Math.floorMod(micros, 1000L) * 1_000_000L + picosOfMicro);
                if (timestampWithTimeZoneType.isShort()) {
                    timestampWithTimeZoneType.writeLong(timestampWithTimeZone, DateTimeEncoding.packDateTimeWithZone(millis, UTC_KEY));
                }
                else {
                    timestampWithTimeZoneType.writeObject(timestampWithTimeZone, LongTimestampWithTimeZone.fromEpochMillisAndFraction(millis, picosOfMilli, UTC_KEY));
                }
            }

            columns.add(typed("time_" + precision, timeType));
            columns.add(typed("time_tz_" + precision, timeWithTimeZoneType));
            columns.add(typed("timestamp_" + precision, timestampType));
            columns.add(typed("timestamp_tz_" + precision, timestampWithTimeZoneType));
            blocks.add(time.build(), timeWithTimeZone.build(), timestamp.build(), timestampWithTimeZone.build());
        }
        List<TypedColumn> typedColumns = columns.build();
        Page page = new Page(blocks.build().toArray(new Block[0]));

        List<List<Object>> result = roundTrip(typedColumns, page);
        assertThat(result).isEqualTo(jsonRoundTrip(typedColumns, page));
        assertThat(result.get(1)).containsExactly(
                "01:02:03",
                "01:02:03+00:00",
                "1969-12-31 23:59:59",
                "1969-12-31 23:59:59 UTC",
                "01:02:03.400",
                "01:02:03.400+00:00",
                "1969-12-31 23:59:59.999",
                "1969-12-31 23:59:59.999 UTC",
                "01:02:03.400000",
                "01:02:03.400000+00:00",
                "1969-12-31 23:59:59.999999",
                "1969-12-31 23:59:59.999999 UTC",
                "01:02:03.400000000",
                "01:02:03.400000000+00:00",
                "1969-12-31 23:59:59.999999123",
                "1969-12-31 23:59:59.999999123 UTC");
    }

    @Test
    public void testDecimalTextMatchesJsonEncoding()
            throws IOException
    {
        DecimalType shortDecimal = createDecimalType(10, 3);
        BlockBuilder shortBlock = shortDecimal.createBlockBuilder(null, 6);
        for (long unscaled : new long[] {123_456L, 1_000L, -5L, 0L, 9_999_999_999L}) {
            shortDecimal.writeLong(shortBlock, unscaled);
        }
        shortBlock.appendNull();

        DecimalType longDecimal = createDecimalType(38, 20);
        BlockBuilder longBlock = longDecimal.createBlockBuilder(null, 6);
        for (String unscaled : new String[] {"1", "-1", "100000000000000000000", "0", "99999999999999999999999999999999999999"}) {
            longDecimal.writeObject(longBlock, Int128.valueOf(new BigInteger(unscaled)));
        }
        longBlock.appendNull();

        DecimalType integralDecimal = createDecimalType(5, 0);
        BlockBuilder integralBlock = integralDecimal.createBlockBuilder(null, 6);
        for (long unscaled : new long[] {12_345L, -1L, 0L, 10L, 99_999L}) {
            integralDecimal.writeLong(integralBlock, unscaled);
        }
        integralBlock.appendNull();

        List<TypedColumn> columns = ImmutableList.of(typed("short", shortDecimal), typed("long", longDecimal), typed("integral", integralDecimal));
        Page page = page(shortBlock.build(), longBlock.build(), integralBlock.build());

        // Decimals keep the trailing zeros of their scale and never use an exponent
        assertThat(roundTrip(columns, page))
                .containsExactly(
                        List.of("123.456", "0.00000000000000000001", "12345"),
                        List.of("1.000", "-0.00000000000000000001", "-1"),
                        List.of("-0.005", "1.00000000000000000000", "0"),
                        List.of("0.000", "0.00000000000000000000", "10"),
                        List.of("9999999.999", "999999999999999999.99999999999999999999", "99999"),
                        Arrays.asList(null, null, null))
                .isEqualTo(jsonRoundTrip(columns, page));
    }

    @Test
    public void testUuidSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", UUID));
        BlockBuilder blockBuilder = UUID.createBlockBuilder(null, 2);

        UUID uuid1 = java.util.UUID.randomUUID();
        UUID uuid2 = java.util.UUID.randomUUID();

        UUID.writeSlice(blockBuilder, Slices.wrappedBuffer(uuidToBytes(uuid1)));
        UUID.writeSlice(blockBuilder, Slices.wrappedBuffer(uuidToBytes(uuid2)));
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
        assertThat(result.get(0).get(0)).isEqualTo(uuid1.toString());
        assertThat(result.get(1).get(0)).isEqualTo(uuid2.toString());
    }

    @Test
    public void testJsonSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", JsonType.JSON));
        BlockBuilder blockBuilder = JsonType.JSON.createBlockBuilder(null, 3);

        // Test various JSON values
        JsonType.JSON.writeSlice(blockBuilder, utf8Slice("{\"name\":\"Alice\",\"age\":30}"));
        JsonType.JSON.writeSlice(blockBuilder, utf8Slice("[1,2,3,4,5]"));
        JsonType.JSON.writeSlice(blockBuilder, utf8Slice("\"simple string\""));

        Page page = page(blockBuilder.build());
        List<List<Object>> result = roundTrip(columns, page);

        assertThat(result).containsExactly(
                List.of("{\"name\":\"Alice\",\"age\":30}"),
                List.of("[1,2,3,4,5]"),
                List.of("\"simple string\""));
    }

    @Test
    public void testEncodedJsonSerialization()
            throws IOException
    {
        // JSON values parsed by the engine are stored in the typed encoding rather than as raw text
        List<TypedColumn> columns = ImmutableList.of(typed("col0", JsonType.JSON));
        BlockBuilder blockBuilder = JsonType.JSON.createBlockBuilder(null, 3);
        JsonType.JSON.writeObject(blockBuilder, JsonItems.fromText(utf8Slice("{\"name\": \"Alice\", \"tags\": [1, 2.5, null, true]}")));
        JsonType.JSON.writeObject(blockBuilder, JsonItems.fromText(utf8Slice("\"text\"")));
        blockBuilder.appendNull();
        Block block = blockBuilder.build();

        assertThat(roundTrip(columns, page(block))).containsExactly(
                List.of(JsonType.JSON.getObjectValue(block, 0)),
                List.of(JsonType.JSON.getObjectValue(block, 1)),
                Arrays.asList((Object) null));
    }

    @Test
    public void testSimpleRowFromSQL()
            throws IOException
    {
        // Test "SELECT ROW(1, 'one')"
        RowType rowType = RowType.rowType(
                RowType.field("field0", BIGINT),
                RowType.field("field1", VARCHAR));

        List<TypedColumn> columns = ImmutableList.of(typed("_col0", rowType));
        RowBlockBuilder blockBuilder = rowType.createBlockBuilder(null, 1);

        // Build ROW(1, 'one')
        blockBuilder.buildEntry(builders -> {
            BIGINT.writeLong(builders.get(0), 1L);
            VARCHAR.writeSlice(builders.get(1), utf8Slice("one"));
        });

        Page page = page(blockBuilder.build());
        List<List<Object>> result = roundTrip(columns, page);

        assertThat(result)
                .containsExactly(List.of(Row.builder().addField("field0", 1L).addField("field1", "one").build()))
                .isEqualTo(jsonRoundTrip(columns, page));
    }

    @Test
    public void testAnonymousRowSerialization()
            throws IOException
    {
        // Test anonymous ROW type - like SELECT ROW(42, 'hello', true)
        RowType rowType = RowType.anonymous(ImmutableList.of(BIGINT, VARCHAR, BOOLEAN));

        List<TypedColumn> columns = ImmutableList.of(typed("col0", rowType));
        RowBlockBuilder blockBuilder = rowType.createBlockBuilder(null, 2);

        // First row: ROW(42, 'hello', true)
        blockBuilder.buildEntry(builders -> {
            BIGINT.writeLong(builders.get(0), 42L);
            VARCHAR.writeSlice(builders.get(1), utf8Slice("hello"));
            BOOLEAN.writeBoolean(builders.get(2), true);
        });

        // Second row: ROW(123, 'world', false)
        blockBuilder.buildEntry(builders -> {
            BIGINT.writeLong(builders.get(0), 123L);
            VARCHAR.writeSlice(builders.get(1), utf8Slice("world"));
            BOOLEAN.writeBoolean(builders.get(2), false);
        });

        Page page = page(blockBuilder.build());
        List<List<Object>> result = roundTrip(columns, page);

        // Anonymous fields are unnamed, as with the JSON encoding
        assertThat(result)
                .containsExactly(
                        List.of(Row.builder().addUnnamedField(42L).addUnnamedField("hello").addUnnamedField(true).build()),
                        List.of(Row.builder().addUnnamedField(123L).addUnnamedField("world").addUnnamedField(false).build()))
                .isEqualTo(jsonRoundTrip(columns, page));
    }

    @Test
    public void testArraySerialization()
            throws IOException
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", arrayType));
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 2);

        blockBuilder.buildEntry(builder -> {
            BIGINT.writeLong(builder, 1L);
            BIGINT.writeLong(builder, 2L);
            BIGINT.writeLong(builder, 3L);
        });
        blockBuilder.buildEntry(builder -> {
            BIGINT.writeLong(builder, 4L);
            BIGINT.writeLong(builder, 5L);
        });

        Page page = page(blockBuilder.build());
        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testArraySerializationWithDictionaryBlock()
            throws IOException
    {
        // Ensures ArrayWriter handles DictionaryBlock-wrapped arrays (e.g. after hash joins)
        ArrayType arrayType = new ArrayType(BIGINT);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", arrayType));
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 3);

        blockBuilder.buildEntry(builder -> {
            BIGINT.writeLong(builder, 10L);
            BIGINT.writeLong(builder, 20L);
        });
        blockBuilder.buildEntry(builder -> {
            BIGINT.writeLong(builder, 30L);
            BIGINT.writeLong(builder, 40L);
        });
        blockBuilder.buildEntry(builder -> {
            BIGINT.writeLong(builder, 50L);
        });

        Block rawBlock = blockBuilder.build();
        // Reorder via dictionary indices: [2, 0, 1]
        Block dictionaryBlock = DictionaryBlock.create(3, rawBlock, new int[] {2, 0, 1});

        List<List<Object>> result = roundTrip(columns, page(dictionaryBlock));

        assertThat(result).containsExactly(
                List.of(List.of(50L)),
                List.of(List.of(10L, 20L)),
                List.of(List.of(30L, 40L)));
    }

    @Test
    public void testArraySerializationWithRunLengthEncodedBlock()
            throws IOException
    {
        // Ensures ArrayWriter handles RunLengthEncodedBlock-wrapped arrays (e.g. constant columns)
        ArrayType arrayType = new ArrayType(BIGINT);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", arrayType));
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 1);

        blockBuilder.buildEntry(builder -> {
            BIGINT.writeLong(builder, 100L);
            BIGINT.writeLong(builder, 200L);
        });

        Block rleBlock = RunLengthEncodedBlock.create(blockBuilder.build(), 4);

        List<List<Object>> result = roundTrip(columns, page(rleBlock));

        assertThat(result).hasSize(4);
        assertThat(result).allMatch(row -> row.equals(List.of(List.of(100L, 200L))));
    }

    @Test
    public void testScalarDictionaryBlockRoundTrip()
            throws IOException
    {
        // Simulates hash join probe-side output: LookupJoinPageBuilder.build() wraps
        // probe blocks in DictionaryBlock via Block.getPositions() when indices are non-sequential
        List<TypedColumn> columns = ImmutableList.of(typed("col0", BIGINT), typed("col1", VARCHAR));

        // Build underlying value blocks (the "probe page" that the join references)
        BlockBuilder longBuilder = BIGINT.createFixedSizeBlockBuilder(5);
        for (long v : new long[] {100L, 200L, 300L, 400L, 500L}) {
            BIGINT.writeLong(longBuilder, v);
        }
        Block longBlock = longBuilder.build();

        Block varcharBlock = createStringsBlock("alpha", "beta", "gamma", "delta", "epsilon");

        // Wrap in DictionaryBlock with non-sequential indices (as hash join would produce)
        // This selects rows: [2, 0, 4, 2] -> [300/gamma, 100/alpha, 500/epsilon, 300/gamma]
        int[] probeIndices = {2, 0, 4, 2};
        Block dictLongs = DictionaryBlock.create(4, longBlock, probeIndices);
        Block dictVarchars = DictionaryBlock.create(4, varcharBlock, probeIndices);

        // Verify these are actually DictionaryBlock instances
        assertThat(dictLongs).isInstanceOf(DictionaryBlock.class);
        assertThat(dictVarchars).isInstanceOf(DictionaryBlock.class);

        List<List<Object>> result = roundTrip(columns, page(dictLongs, dictVarchars));

        assertThat(result).containsExactly(
                List.of(300L, "gamma"),
                List.of(100L, "alpha"),
                List.of(500L, "epsilon"),
                List.of(300L, "gamma"));
    }

    @Test
    public void testNestedArraySerialization()
            throws IOException
    {
        // Test ARRAY<ARRAY<BIGINT>> - this should reproduce the "Lists have one child Field. Found: none" error
        ArrayType innerArrayType = new ArrayType(BIGINT);
        ArrayType outerArrayType = new ArrayType(innerArrayType);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", outerArrayType));

        ArrayBlockBuilder outerBuilder = outerArrayType.createBlockBuilder(null, 1);

        // Build nested array: [[1, 2, 3]]
        outerBuilder.buildEntry(outerArray -> {
            ArrayBlockBuilder innerBuilder = innerArrayType.createBlockBuilder(null, 1);
            innerBuilder.buildEntry(innerArray -> {
                BIGINT.writeLong(innerArray, 1L);
                BIGINT.writeLong(innerArray, 2L);
                BIGINT.writeLong(innerArray, 3L);
            });
            Block innerBlock = innerBuilder.build();
            innerArrayType.writeObject(outerArray, innerArrayType.getObject(innerBlock, 0));
        });

        Page page = page(outerBuilder.build());
        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(1);
    }

    @Test
    public void testArrayOfUuidSerialization()
            throws IOException
    {
        // Test ARRAY<UUID> - this should also test the UUID metadata issue we discussed
        ArrayType arrayType = new ArrayType(UUID);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", arrayType));
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 1);

        blockBuilder.buildEntry(builder -> {
            java.util.UUID uuid1 = java.util.UUID.randomUUID();
            java.util.UUID uuid2 = java.util.UUID.randomUUID();

            UUID.writeSlice(builder, Slices.wrappedBuffer(uuidToBytes(uuid1)));
            UUID.writeSlice(builder, Slices.wrappedBuffer(uuidToBytes(uuid2)));
        });

        Page page = page(blockBuilder.build());
        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(1);
    }

    @Test
    public void testMapSerialization()
            throws IOException
    {
        MapType mapType = new MapType(VARCHAR, BIGINT, new TypeOperators());
        List<TypedColumn> columns = ImmutableList.of(typed("col0", mapType));
        MapBlockBuilder blockBuilder = mapType.createBlockBuilder(null, 2);

        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            VARCHAR.writeSlice(keyBuilder, utf8Slice("key1"));
            BIGINT.writeLong(valueBuilder, 100L);
            VARCHAR.writeSlice(keyBuilder, utf8Slice("key2"));
            BIGINT.writeLong(valueBuilder, 200L);
        });

        Page page = page(blockBuilder.build());
        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(1);
    }

    @Test
    public void testMapEntriesAreNotNull()
            throws IOException
    {
        MapType mapType = new MapType(VARCHAR, BIGINT, new TypeOperators());
        MapBlockBuilder blockBuilder = mapType.createBlockBuilder(null, 4);
        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            VARCHAR.writeSlice(keyBuilder, utf8Slice("a"));
            BIGINT.writeLong(valueBuilder, 1L);
            VARCHAR.writeSlice(keyBuilder, utf8Slice("b"));
            valueBuilder.appendNull();
        });
        blockBuilder.appendNull();
        blockBuilder.buildEntry((_, _) -> {});
        blockBuilder.buildEntry((keyBuilder, valueBuilder) -> {
            VARCHAR.writeSlice(keyBuilder, utf8Slice("c"));
            BIGINT.writeLong(valueBuilder, 3L);
        });
        Block block = blockBuilder.build();

        // Arrow readers like pyarrow reject maps with null entries, only the map itself or its values can be null
        assertMapEntries(mapType, block, 1, 3);
        assertMapEntries(mapType, DictionaryBlock.create(4, block, new int[] {3, 1, 0, 0}), 1, 5);

        Map<Object, Object> firstMap = new HashMap<>();
        firstMap.put("a", 1L);
        firstMap.put("b", null);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", mapType));
        assertThat(roundTrip(columns, page(block)))
                .containsExactly(
                        List.of(firstMap),
                        Arrays.asList((Object) null),
                        List.of(Map.of()),
                        List.of(Map.of("c", 3L)))
                .isEqualTo(jsonRoundTrip(columns, page(block)));
    }

    private void assertMapEntries(MapType mapType, Block block, int expectedNullMaps, int expectedEntries)
    {
        try (MapVector vector = (MapVector) toArrowField("col0", mapType, true).createVector(allocator)) {
            writerForVector(vector, mapType).write(block);
            assertThat(vector.getValueCount()).isEqualTo(block.getPositionCount());
            assertThat(vector.getNullCount()).isEqualTo(expectedNullMaps);

            StructVector entries = (StructVector) vector.getDataVector();
            assertThat(entries.getValueCount()).isEqualTo(expectedEntries);
            assertThat(entries.getNullCount()).isZero();
            assertThat(entries.getChild("key").getNullCount()).isZero();
        }
    }

    @Test
    public void testRowSerialization()
            throws IOException
    {
        RowType rowType = RowType.rowType(
                RowType.field("a", BIGINT),
                RowType.field("b", VARCHAR),
                RowType.field("c", BOOLEAN));

        List<TypedColumn> columns = ImmutableList.of(typed("col0", rowType));
        RowBlockBuilder blockBuilder = rowType.createBlockBuilder(null, 3);

        blockBuilder.buildEntry(builders -> {
            BIGINT.writeLong(builders.get(0), 123L);
            VARCHAR.writeSlice(builders.get(1), utf8Slice("test"));
            BOOLEAN.writeBoolean(builders.get(2), true);
        });

        blockBuilder.buildEntry(builders -> {
            builders.get(0).appendNull();
            builders.get(1).appendNull();
            builders.get(2).appendNull();
        });
        blockBuilder.appendNull();

        Page page = page(blockBuilder.build());
        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result)
                .containsExactly(
                        List.of(Row.builder().addField("a", 123L).addField("b", "test").addField("c", true).build()),
                        List.of(Row.builder().addField("a", null).addField("b", null).addField("c", null).build()),
                        Arrays.asList((Object) null))
                .isEqualTo(jsonRoundTrip(columns, page));
    }

    @Test
    public void testRowSerializationWithValues()
            throws IOException
    {
        RowType rowType = RowType.rowType(
                RowType.field("id", BIGINT),
                RowType.field("name", VARCHAR),
                RowType.field("active", BOOLEAN));

        List<TypedColumn> columns = ImmutableList.of(typed("col0", rowType));
        RowBlockBuilder blockBuilder = rowType.createBlockBuilder(null, 3);

        // First row: {id: 1001, name: "Alice", active: true}
        blockBuilder.buildEntry(builders -> {
            BIGINT.writeLong(builders.get(0), 1001L);
            VARCHAR.writeSlice(builders.get(1), utf8Slice("Alice"));
            BOOLEAN.writeBoolean(builders.get(2), true);
        });

        // Second row: {id: 2002, name: "Bob", active: false}
        blockBuilder.buildEntry(builders -> {
            BIGINT.writeLong(builders.get(0), 2002L);
            VARCHAR.writeSlice(builders.get(1), utf8Slice("Bob"));
            BOOLEAN.writeBoolean(builders.get(2), false);
        });

        // Third row: {id: null, name: "Charlie", active: null}
        blockBuilder.buildEntry(builders -> {
            builders.get(0).appendNull();
            VARCHAR.writeSlice(builders.get(1), utf8Slice("Charlie"));
            builders.get(2).appendNull();
        });

        Page page = page(blockBuilder.build());
        List<List<Object>> result = roundTrip(columns, page);

        assertThat(result)
                .containsExactly(
                        List.of(Row.builder().addField("id", 1001L).addField("name", "Alice").addField("active", true).build()),
                        List.of(Row.builder().addField("id", 2002L).addField("name", "Bob").addField("active", false).build()),
                        List.of(Row.builder().addField("id", null).addField("name", "Charlie").addField("active", null).build()))
                .isEqualTo(jsonRoundTrip(columns, page));
    }

    @Test
    public void testRecursiveRowSerialization()
            throws IOException
    {
        // Create ROW<address ROW<street VARCHAR, city VARCHAR>, age BIGINT>
        RowType addressType = RowType.rowType(
                RowType.field("street", VARCHAR),
                RowType.field("city", VARCHAR));

        RowType personType = RowType.rowType(
                RowType.field("address", addressType),
                RowType.field("age", BIGINT));

        List<TypedColumn> columns = ImmutableList.of(typed("col0", personType));
        RowBlockBuilder blockBuilder = personType.createBlockBuilder(null, 2);

        // First row: {address: {street: "123 Main St", city: "Springfield"}, age: 30}
        blockBuilder.buildEntry(personBuilders -> {
            // Build the nested address row
            RowBlockBuilder addressBuilder = addressType.createBlockBuilder(null, 1);
            addressBuilder.buildEntry(addressBuilders -> {
                VARCHAR.writeSlice(addressBuilders.get(0), utf8Slice("123 Main St"));
                VARCHAR.writeSlice(addressBuilders.get(1), utf8Slice("Springfield"));
            });
            addressType.writeObject(personBuilders.get(0), addressType.getObject(addressBuilder.build(), 0));
            BIGINT.writeLong(personBuilders.get(1), 30L);
        });

        // Second row: {address: {street: "456 Oak Ave", city: "Riverside"}, age: 25}
        blockBuilder.buildEntry(personBuilders -> {
            // Build the nested address row
            RowBlockBuilder addressBuilder = addressType.createBlockBuilder(null, 1);
            addressBuilder.buildEntry(addressBuilders -> {
                VARCHAR.writeSlice(addressBuilders.get(0), utf8Slice("456 Oak Ave"));
                VARCHAR.writeSlice(addressBuilders.get(1), utf8Slice("Riverside"));
            });
            addressType.writeObject(personBuilders.get(0), addressType.getObject(addressBuilder.build(), 0));
            BIGINT.writeLong(personBuilders.get(1), 25L);
        });

        Page page = page(blockBuilder.build());
        List<List<Object>> result = roundTrip(columns, page);

        assertThat(result)
                .containsExactly(
                        List.of(Row.builder()
                                .addField("address", Row.builder().addField("street", "123 Main St").addField("city", "Springfield").build())
                                .addField("age", 30L)
                                .build()),
                        List.of(Row.builder()
                                .addField("address", Row.builder().addField("street", "456 Oak Ave").addField("city", "Riverside").build())
                                .addField("age", 25L)
                                .build()))
                .isEqualTo(jsonRoundTrip(columns, page));
    }

    @Test
    public void testDeeplyNestedRowSerialization()
            throws IOException
    {
        // Create ROW<person ROW<name ROW<first VARCHAR, last VARCHAR>, age BIGINT>, active BOOLEAN>
        RowType nameType = RowType.rowType(
                RowType.field("first", VARCHAR),
                RowType.field("last", VARCHAR));

        RowType personType = RowType.rowType(
                RowType.field("name", nameType),
                RowType.field("age", BIGINT));

        RowType recordType = RowType.rowType(
                RowType.field("person", personType),
                RowType.field("active", BOOLEAN));

        List<TypedColumn> columns = ImmutableList.of(typed("col0", recordType));
        RowBlockBuilder blockBuilder = recordType.createBlockBuilder(null, 1);

        // Build: {person: {name: {first: "John", last: "Doe"}, age: 35}, active: true}
        blockBuilder.buildEntry(recordBuilders -> {
            // Build the nested person row
            RowBlockBuilder personBuilder = personType.createBlockBuilder(null, 1);
            personBuilder.buildEntry(personBuilders -> {
                // Build the nested name row
                RowBlockBuilder nameBuilder = nameType.createBlockBuilder(null, 1);
                nameBuilder.buildEntry(nameBuilders -> {
                    VARCHAR.writeSlice(nameBuilders.get(0), utf8Slice("John"));
                    VARCHAR.writeSlice(nameBuilders.get(1), utf8Slice("Doe"));
                });
                nameType.writeObject(personBuilders.get(0), nameType.getObject(nameBuilder.build(), 0));
                BIGINT.writeLong(personBuilders.get(1), 35L);
            });
            personType.writeObject(recordBuilders.get(0), personType.getObject(personBuilder.build(), 0));
            BOOLEAN.writeBoolean(recordBuilders.get(1), true);
        });

        Page page = page(blockBuilder.build());
        List<List<Object>> result = roundTrip(columns, page);

        Row name = Row.builder().addField("first", "John").addField("last", "Doe").build();
        Row person = Row.builder().addField("name", name).addField("age", 35L).build();
        assertThat(result)
                .containsExactly(List.of(Row.builder().addField("person", person).addField("active", true).build()))
                .isEqualTo(jsonRoundTrip(columns, page));
    }

    @Test
    public void testIntervalYearMonthSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", IntervalYearMonthType.INTERVAL_YEAR_MONTH));
        BlockBuilder blockBuilder = IntervalYearMonthType.INTERVAL_YEAR_MONTH.createBlockBuilder(null, 4);
        IntervalYearMonthType.INTERVAL_YEAR_MONTH.writeInt(blockBuilder, 15); // 15 months = 1 year 3 months
        IntervalYearMonthType.INTERVAL_YEAR_MONTH.writeInt(blockBuilder, 25); // 25 months = 2 years 1 month
        IntervalYearMonthType.INTERVAL_YEAR_MONTH.writeInt(blockBuilder, -7); // -7 months
        blockBuilder.appendNull();
        Page page = page(blockBuilder.build());

        // Year-month intervals are written as a number of months in a month-day-nano interval, which Arrow readers like pyarrow support
        assertThat(toArrowField("col0", IntervalYearMonthType.INTERVAL_YEAR_MONTH, true).getType())
                .isEqualTo(new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO));

        assertThat(roundTrip(columns, page))
                .containsExactly(
                        List.of("1-3"),
                        List.of("2-1"),
                        List.of("-0-7"),
                        Arrays.asList((Object) null))
                .isEqualTo(jsonRoundTrip(columns, page));
    }

    @Test
    public void testLargeIntervalYearMonthSerialization()
            throws IOException
    {
        // Test for large interval that caused issues in distributed tests
        // SELECT INTERVAL '32767' YEAR -> 32767 * 12 = 393204 months
        List<TypedColumn> columns = ImmutableList.of(typed("col0", IntervalYearMonthType.INTERVAL_YEAR_MONTH));
        BlockBuilder blockBuilder = IntervalYearMonthType.INTERVAL_YEAR_MONTH.createBlockBuilder(null, 1);
        IntervalYearMonthType.INTERVAL_YEAR_MONTH.writeInt(blockBuilder, 393204); // 32767 years = 393204 months
        Page page = page(blockBuilder.build());

        // This should either succeed or fail with a clear Arrow encoding/decoding error
        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(1);
        // The large interval should be encoded as 32767-0 (32767 years, 0 months)
        assertThat(result.get(0).get(0).toString()).isEqualTo("32767-0");
    }

    @Test
    public void testIntervalDayTimeSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", INTERVAL_DAY_TIME));
        Page page = page(intervalDayTimeBlock(INTERVAL_DAY_TIME));

        // Day-time intervals are stored as microseconds and rendered with the fractional precision of the type
        assertThat(roundTrip(columns, page)).containsExactly(
                List.of("1 00:00:00.001000"),
                List.of("2 00:00:00.000002"),
                List.of("-0 00:00:01.500000"),
                List.of("0 00:00:00.000500"),
                Arrays.asList((Object) null));

        // Clients without the parametric interval capability get values rounded to milliseconds, as with the JSON encoding
        assertThat(roundTrip(columns, page, false)).containsExactly(
                List.of("1 00:00:00.001"),
                List.of("2 00:00:00.000"),
                List.of("-0 00:00:01.500"),
                List.of("0 00:00:00.001"),
                Arrays.asList((Object) null));
    }

    @Test
    public void testIntervalDayTimeWithoutFractionalSeconds()
            throws IOException
    {
        IntervalDayTimeType dayToHour = createIntervalDayTimeType(DAY, HOUR);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", dayToHour));
        BlockBuilder blockBuilder = dayToHour.createBlockBuilder(null, 1);
        dayToHour.writeLong(blockBuilder, 26 * 3600 * 1_000_000L); // 1 day, 2 hours
        Page page = page(blockBuilder.build());

        assertThat(roundTrip(columns, page)).containsExactly(List.of("1 02:00:00"));
    }

    @Test
    public void testArrayOfIntervalDayTime()
            throws IOException
    {
        ArrayType arrayType = new ArrayType(INTERVAL_DAY_TIME);
        List<TypedColumn> columns = ImmutableList.of(typed("col0", arrayType));
        ArrayBlockBuilder blockBuilder = arrayType.createBlockBuilder(null, 1);
        blockBuilder.buildEntry(elementBuilder -> {
            INTERVAL_DAY_TIME.writeLong(elementBuilder, 1_000_001L);
            elementBuilder.appendNull();
        });
        Page page = page(blockBuilder.build());

        assertThat(roundTrip(columns, page)).containsExactly(
                List.of(Arrays.asList("0 00:00:01.000001", null)));
    }

    @Test
    public void testUnsupportedPicosecondIntervalDayTime()
    {
        IntervalDayTimeType picosecondInterval = createIntervalDayTimeType(DAY, SECOND, 9, 12);
        List<OutputColumn> columns = ImmutableList.of(
                new OutputColumn(0, "supported", INTERVAL_DAY_TIME),
                new OutputColumn(1, "unsupported", picosecondInterval),
                new OutputColumn(2, "nested", new ArrayType(picosecondInterval)));

        assertThat(new ArrowQueryDataEncoder.Factory(allocator, Optional.empty()).unsupported(columns))
                .extracting(OutputColumn::columnName)
                .containsExactly("unsupported", "nested");
    }

    private static Block intervalDayTimeBlock(IntervalDayTimeType type)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 5);
        type.writeLong(blockBuilder, 86_400_000_000L + 1_000L); // 1 day, 1 millisecond
        type.writeLong(blockBuilder, 2 * 86_400_000_000L + 2L); // 2 days, 2 microseconds
        type.writeLong(blockBuilder, -1_500_000L); // -1.5 seconds
        type.writeLong(blockBuilder, 500L); // half a millisecond, rounded up for legacy clients
        blockBuilder.appendNull();
        return blockBuilder.build();
    }

    @Test
    public void testUnknownTypeSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", UNKNOWN));
        BlockBuilder blockBuilder = UNKNOWN.createBlockBuilder(null, 3);
        blockBuilder.appendNull();
        blockBuilder.appendNull();
        blockBuilder.appendNull();
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(3);
        // All unknown type values should be null
        for (List<Object> row : result) {
            assertThat(row).hasSize(1);
            assertThat(row.get(0)).isNull();
        }
    }

    @Test
    public void testNullValues()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(
                typed("bigint_col", BIGINT),
                typed("varchar_col", VARCHAR),
                typed("boolean_col", BOOLEAN));

        BlockBuilder bigintBuilder = BIGINT.createBlockBuilder(null, 3);
        BIGINT.writeLong(bigintBuilder, 1L);
        bigintBuilder.appendNull();
        BIGINT.writeLong(bigintBuilder, 3L);

        BlockBuilder varcharBuilder = VARCHAR.createBlockBuilder(null, 3);
        VARCHAR.writeSlice(varcharBuilder, utf8Slice("a"));
        varcharBuilder.appendNull();
        VARCHAR.writeSlice(varcharBuilder, utf8Slice("c"));

        BlockBuilder booleanBuilder = BOOLEAN.createBlockBuilder(null, 3);
        BOOLEAN.writeBoolean(booleanBuilder, true);
        booleanBuilder.appendNull();
        BOOLEAN.writeBoolean(booleanBuilder, false);

        Page page = new Page(bigintBuilder.build(), varcharBuilder.build(), booleanBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(3);

        // First row
        assertThat(result.get(0)).hasSize(3);
        assertThat(result.get(0).get(0)).isEqualTo(1L);
        assertThat(result.get(0).get(1)).isEqualTo("a");
        assertThat(result.get(0).get(2)).isEqualTo(true);

        // Second row - all nulls
        assertThat(result.get(1)).hasSize(3);
        assertThat(result.get(1).get(0)).isNull();
        assertThat(result.get(1).get(1)).isNull();
        assertThat(result.get(1).get(2)).isNull();

        // Third row
        assertThat(result.get(2)).hasSize(3);
        assertThat(result.get(2).get(0)).isEqualTo(3L);
        assertThat(result.get(2).get(1)).isEqualTo("c");
        assertThat(result.get(2).get(2)).isEqualTo(false);
    }

    @Test
    public void testMultipleColumnTypes()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(
                typed("bigint_col", BIGINT),
                typed("varchar_col", VARCHAR),
                typed("boolean_col", BOOLEAN));

        Page page = new Page(
                createTypedLongsBlock(BIGINT, 1L, 2L, 3L),
                createStringsBlock("a", "b", "c"),
                createBooleansBlock(true, false, true));

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).containsExactly(
                List.of(1L, "a", true),
                List.of(2L, "b", false),
                List.of(3L, "c", true));
    }

    @Test
    public void testSegmentSizeMatchesEncodedBytes()
            throws IOException
    {
        List<OutputColumn> columns = ImmutableList.of(new OutputColumn(0, "col0", BIGINT), new OutputColumn(1, "col1", VARCHAR));
        Page page = page(createTypedLongsBlock(BIGINT, 1L, 2L, 3L), createStringsBlock("a", "b", "c"));
        List<QueryDataEncoder.Factory> factories = ImmutableList.of(
                new ArrowQueryDataEncoder.Factory(allocator, Optional.empty()),
                new ArrowQueryDataEncoder.ZstdFactory(allocator, new ArrowCompressionFactory(), Optional.empty()));

        for (QueryDataEncoder.Factory factory : factories) {
            for (List<Page> pages : ImmutableList.of(ImmutableList.<Page>of(), ImmutableList.of(page), ImmutableList.of(page, page))) {
                QueryDataEncoder encoder = factory.create(TEST_SESSION, columns);
                try {
                    ByteArrayOutputStream output = new ByteArrayOutputStream();
                    DataAttributes attributes = encoder.encodeTo(output, pages);
                    // The reported size covers the whole stream, including the end-of-stream marker
                    assertThat(attributes.get(SEGMENT_SIZE, Integer.class)).isEqualTo(output.size());
                }
                finally {
                    encoder.close();
                }
            }
        }
    }

    @Test
    public void testOutputStreamFailuresAreReportedAsIOException()
    {
        List<OutputColumn> columns = ImmutableList.of(new OutputColumn(0, "col0", BIGINT));
        List<Page> pages = ImmutableList.of(page(createTypedLongsBlock(BIGINT, 1L, 2L, 3L)));

        // Spooling storages complete the upload of a segment when its output stream is closed. Both failures
        // must surface as IOException for the spooling operator to fall back to inlining the segment.
        assertEncodingFails(columns, pages, new FailingOutputStream(true, false), "write failed");
        assertEncodingFails(columns, pages, new FailingOutputStream(false, true), "close failed");
    }

    private void assertEncodingFails(List<OutputColumn> columns, List<Page> pages, OutputStream output, String expectedMessage)
    {
        QueryDataEncoder encoder = createEncoder(columns);
        try {
            assertThatThrownBy(() -> encoder.encodeTo(output, pages))
                    .isInstanceOf(IOException.class)
                    .hasMessage(expectedMessage);
        }
        finally {
            // Fails if any Arrow buffer was leaked by the failed encoding
            encoder.close();
        }
    }

    private static class FailingOutputStream
            extends OutputStream
    {
        private final boolean failOnWrite;
        private final boolean failOnClose;

        public FailingOutputStream(boolean failOnWrite, boolean failOnClose)
        {
            this.failOnWrite = failOnWrite;
            this.failOnClose = failOnClose;
        }

        @Override
        public void write(int value)
                throws IOException
        {
            write(new byte[] {(byte) value}, 0, 1);
        }

        @Override
        public void write(byte[] buffer, int offset, int length)
                throws IOException
        {
            if (failOnWrite) {
                throw new IOException("write failed");
            }
        }

        @Override
        public void close()
                throws IOException
        {
            if (failOnClose) {
                throw new IOException("close failed");
            }
        }
    }

    protected List<List<Object>> roundTrip(List<TypedColumn> columns, Page page)
            throws IOException
    {
        return roundTrip(columns, page, true);
    }

    private List<List<Object>> roundTrip(List<TypedColumn> columns, Page page, boolean supportsParametricInterval)
            throws IOException
    {
        QueryDataEncoder encoder = newEncoder(columns);
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            encoder.encodeTo(output, List.of(page));

            QueryDataDecoder decoder = newDecoder(columns, supportsParametricInterval);
            try (CloseableIterator<List<Object>> iterator = decoder.decode(new ByteArrayInputStream(output.toByteArray()), null)) {
                return ImmutableList.copyOf(iterator);
            }
        }
        finally {
            encoder.close();
        }
    }

    private static List<List<Object>> jsonRoundTrip(List<TypedColumn> columns, Page page)
            throws IOException
    {
        ImmutableList.Builder<OutputColumn> outputColumns = ImmutableList.builder();
        ImmutableList.Builder<Column> clientColumns = ImmutableList.builder();
        for (int i = 0; i < columns.size(); i++) {
            TypedColumn column = columns.get(i);
            outputColumns.add(new OutputColumn(i, column.name(), column.type()));
            clientColumns.add(createColumn(column.name(), column.type(), true, true, false, false, true));
        }
        QueryDataEncoder encoder = new JsonQueryDataEncoder.Factory(new JsonMapper()).create(TEST_SESSION, outputColumns.build());
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            encoder.encodeTo(output, List.of(page));
            QueryDataDecoder decoder = new JsonQueryDataDecoder.Factory().create(clientColumns.build(), DataAttributes.empty(), false);
            try (CloseableIterator<List<Object>> iterator = decoder.decode(new ByteArrayInputStream(output.toByteArray()), null)) {
                return ImmutableList.copyOf(iterator);
            }
        }
        finally {
            encoder.close();
        }
    }

    record TypedColumn(String name, Type type)
    {
        public TypedColumn
        {
            requireNonNull(name, "name is null");
            requireNonNull(type, "type is null");
        }

        public static TypedColumn typed(String name, Type type)
        {
            return new TypedColumn(name, type);
        }
    }

    private static TypedColumn typed(String name, Type type)
    {
        return new TypedColumn(name, type);
    }

    private QueryDataEncoder newEncoder(List<TypedColumn> types)
    {
        ImmutableList.Builder<OutputColumn> columns = ImmutableList.builderWithExpectedSize(types.size());
        for (int i = 0; i < types.size(); i++) {
            TypedColumn typedColumn = types.get(i);
            columns.add(new OutputColumn(i, typedColumn.name(), typedColumn.type()));
        }
        return createEncoder(columns.build());
    }

    private QueryDataDecoder newDecoder(List<TypedColumn> types, boolean supportsParametricInterval)
    {
        ImmutableList.Builder<Column> columns = ImmutableList.builderWithExpectedSize(types.size());
        for (TypedColumn typedColumn : types) {
            columns.add(createColumn(typedColumn.name(), typedColumn.type(), true, true, false, false, supportsParametricInterval));
        }
        return createDecoder(columns.build());
    }

    private static Page page(Block... blocks)
    {
        return new Page(blocks);
    }

    private static long packTimeWithTimeZone(long timeMillis, int offsetMinutes)
    {
        return (((long) offsetMinutes) << 40) | timeMillis;
    }

    private static long packDateTimeWithZone(long epochMillis, int offsetMinutes)
    {
        return (epochMillis << 12) | (offsetMinutes & 0xFFF);
    }

    private static byte[] uuidToBytes(UUID uuid)
    {
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }
}
