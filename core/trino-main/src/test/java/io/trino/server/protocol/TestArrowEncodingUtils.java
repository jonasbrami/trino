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
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.encoding.ArrowQueryDataDecoder;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.server.protocol.spooling.encoding.ArrowQueryDataEncoder;
import io.trino.spi.Page;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import io.trino.type.IntervalDayTimeType;
import io.trino.type.UnknownType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.block.BlockAssertions.createTypedLongsBlock;
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createSmallintsBlock;
import static io.trino.block.BlockAssertions.createTinyintsBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.block.BlockAssertions.createCharsBlock;
import static io.trino.block.BlockAssertions.createDateSequenceBlock;
import static io.trino.block.BlockAssertions.createLongDecimalsBlock;
import static io.trino.block.BlockAssertions.createTimestampSequenceBlock;
import static io.trino.server.protocol.ProtocolUtil.createColumn;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeType.TIME_NANOS;
import static io.trino.spi.type.TimeType.TIME_SECONDS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MILLIS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_MICROS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_NANOS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_SECONDS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_SECONDS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
import static io.trino.spi.type.UuidType.UUID;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

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
        return new ArrowQueryDataDecoder.Factory().create(columns, DataAttributes.empty());
    }

    protected QueryDataEncoder createEncoder(List<OutputColumn> columns)
    {
        return new ArrowQueryDataEncoder.Factory(allocator).create(TEST_SESSION, columns);
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
        TIME_SECONDS.writeLong(blockBuilder, 12345L); // picoseconds since midnight
        TIME_SECONDS.writeLong(blockBuilder, 67890L);
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
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
    public void testRowSerialization()
            throws IOException
    {
        RowType rowType = RowType.rowType(
                RowType.field("a", BIGINT),
                RowType.field("b", VARCHAR),
                RowType.field("c", BOOLEAN));

        List<TypedColumn> columns = ImmutableList.of(typed("col0", rowType));
        RowBlockBuilder blockBuilder = rowType.createBlockBuilder(null, 2);

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

        Page page = page(blockBuilder.build());
        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testIntervalDayTimeSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", IntervalDayTimeType.INTERVAL_DAY_TIME));
        BlockBuilder blockBuilder = IntervalDayTimeType.INTERVAL_DAY_TIME.createBlockBuilder(null, 2);
        IntervalDayTimeType.INTERVAL_DAY_TIME.writeLong(blockBuilder, 0x0000000100000001L); // 1 day, 1 millisecond
        IntervalDayTimeType.INTERVAL_DAY_TIME.writeLong(blockBuilder, 0x0000000200000002L); // 2 days, 2 milliseconds
        Page page = page(blockBuilder.build());

        List<List<Object>> result = roundTrip(columns, page);
        assertThat(result).hasSize(2);
    }

    @Test
    public void testUnknownTypeSerialization()
            throws IOException
    {
        List<TypedColumn> columns = ImmutableList.of(typed("col0", UnknownType.UNKNOWN));
        BlockBuilder blockBuilder = UnknownType.UNKNOWN.createBlockBuilder(null, 3);
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

    protected List<List<Object>> roundTrip(List<TypedColumn> columns, Page page)
            throws IOException
    {
        QueryDataEncoder encoder = newEncoder(columns);
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            encoder.encodeTo(output, List.of(page));

            QueryDataDecoder decoder = newDecoder(columns);
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

    private QueryDataDecoder newDecoder(List<TypedColumn> types)
    {
        ImmutableList.Builder<Column> columns = ImmutableList.builderWithExpectedSize(types.size());
        for (TypedColumn typedColumn : types) {
            columns.add(createColumn(typedColumn.name(), typedColumn.type(), true));
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
