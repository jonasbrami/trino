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
package io.trino.client.spooling.encoding;

import com.google.common.collect.ImmutableList;
import io.trino.client.ClientTypeSignature;
import io.trino.client.ClientTypeSignatureParameter;
import io.trino.client.Column;
import io.trino.client.Row;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.TransferPair;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.client.ClientStandardTypes.ARRAY;
import static io.trino.client.ClientStandardTypes.BIGINT;
import static io.trino.client.ClientStandardTypes.BOOLEAN;
import static io.trino.client.ClientStandardTypes.CHAR;
import static io.trino.client.ClientStandardTypes.DATE;
import static io.trino.client.ClientStandardTypes.DECIMAL;
import static io.trino.client.ClientStandardTypes.DOUBLE;
import static io.trino.client.ClientStandardTypes.INTEGER;
import static io.trino.client.ClientStandardTypes.INTERVAL_DAY_TO_SECOND;
import static io.trino.client.ClientStandardTypes.INTERVAL_YEAR_TO_MONTH;
import static io.trino.client.ClientStandardTypes.JSON;
import static io.trino.client.ClientStandardTypes.MAP;
import static io.trino.client.ClientStandardTypes.REAL;
import static io.trino.client.ClientStandardTypes.ROW;
import static io.trino.client.ClientStandardTypes.SMALLINT;
import static io.trino.client.ClientStandardTypes.TIME;
import static io.trino.client.ClientStandardTypes.TIMESTAMP;
import static io.trino.client.ClientStandardTypes.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.client.ClientStandardTypes.TIME_WITH_TIME_ZONE;
import static io.trino.client.ClientStandardTypes.TINYINT;
import static io.trino.client.ClientStandardTypes.UUID;
import static io.trino.client.ClientStandardTypes.VARCHAR;
import static io.trino.client.IntervalDayTime.formatMicros;
import static io.trino.client.IntervalDayTime.formatMillis;
import static io.trino.client.IntervalYearMonth.formatMonths;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static org.apache.arrow.vector.types.TimeUnit.MICROSECOND;

public class ArrowDecodingUtils
{
    private static final long MICROSECONDS_PER_SECOND = 1_000_000;
    private static final long NANOSECONDS_PER_MICROSECOND = 1_000;
    private static final long NANOSECONDS_PER_MILLISECOND = 1_000_000;
    private static final long NANOSECONDS_PER_SECOND = 1_000_000_000;

    private ArrowDecodingUtils() {}

    public static VectorTypeDecoder[] createVectorTypeDecoders(List<Column> columns, BufferAllocator allocator, List<FieldVector> vectors)
    {
        verify(!columns.isEmpty(), "Columns must not be empty");
        verify(columns.size() == vectors.size(), "Expected %s vectors, but got %s", columns.size(), vectors.size());
        VectorTypeDecoder[] decoders = new VectorTypeDecoder[columns.size()];
        // Decoders only hold the transferred vectors, which are released if any decoder cannot be created
        List<ValueVector> transferredVectors = new ArrayList<>(columns.size());
        try {
            for (int i = 0; i < columns.size(); i++) {
                TransferPair transferPair = vectors.get(i).getTransferPair(allocator);
                transferredVectors.add(transferPair.getTo());
                transferPair.getTo().allocateNew();
                transferPair.transfer();
                decoders[i] = createVectorTypeDecoder(columns.get(i).getTypeSignature(), transferPair.getTo());
            }
            return decoders;
        }
        catch (Throwable failure) {
            for (ValueVector vector : transferredVectors) {
                try {
                    vector.close();
                }
                catch (Throwable e) {
                    if (e != failure) {
                        failure.addSuppressed(e);
                    }
                }
            }
            throw failure;
        }
    }

    private static VectorTypeDecoder createVectorTypeDecoder(ClientTypeSignature signature, ValueVector valueVector)
    {
        ValueVector vector = valueVector;
        if (vector instanceof ExtensionTypeVector) {
            // Extension types registered in the JVM, like arrow.uuid for UUID columns, are read from their storage vector
            vector = ((ExtensionTypeVector<?>) vector).getUnderlyingVector();
        }
        switch (signature.getRawType()) {
            case BIGINT:
                return new BigintDecoder(checkedCast(vector, BigIntVector.class));
            case INTEGER:
                return new PassThroughDecoder(vector);
            case SMALLINT:
                return new PassThroughDecoder(vector);
            case TINYINT:
                return new PassThroughDecoder(vector);
            case DOUBLE:
                return new PassThroughDecoder(vector);
            case REAL:
                return new PassThroughDecoder(vector);
            case BOOLEAN:
                return new PassThroughDecoder(vector);
            case VARCHAR:
            case CHAR:
                return new VarcharDecoder(checkedCast(vector, VarCharVector.class));
            case MAP:
                return new MapDecoder(signature, checkedCast(vector, MapVector.class));
            case ARRAY:
                return new ArrayDecoder(signature, checkedCast(vector, ListVector.class));
            case TIMESTAMP:
                return new TimestampDecoder(checkedCast(vector, TimeStampVector.class));
            case DATE:
                return new DateDecoder(checkedCast(vector, DateDayVector.class));
            case UUID:
                return new UuidDecoder(checkedCast(vector, FixedSizeBinaryVector.class));
            case DECIMAL:
                return new DecimalDecoder(checkedCast(vector, DecimalVector.class));
            case INTERVAL_DAY_TO_SECOND:
                return new IntervalDayTimeDecoder(signature, checkedCast(vector, DurationVector.class));
            case INTERVAL_YEAR_TO_MONTH:
                return new IntervalYearMonthDecoder(checkedCast(vector, IntervalMonthDayNanoVector.class));
            case TIME:
                return new TimeDecoder(vector, false);
            case TIME_WITH_TIME_ZONE:
                return new TimeDecoder(vector, true);
            case TIMESTAMP_WITH_TIME_ZONE:
                return new TimestampWithTimeZoneDecoder(checkedCast(vector, TimeStampVector.class));
            case ROW:
                return new RowDecoder(signature, checkedCast(vector, StructVector.class));
            case JSON:
                return new JsonDecoder(checkedCast(vector, VarCharVector.class));

//            case IPADDRESS:
//            case GEOMETRY:
//            case SPHERICAL_GEOGRAPHY:
//            case COLOR:
//            case KDB_TREE:
//            case BING_TILE:
//            case QDIGEST:
//            case P4_HYPER_LOG_LOG:
//            case HYPER_LOG_LOG:
//            case SET_DIGEST:
            default:
                return new PassThroughDecoder(vector);
        }
    }

    private static class PassThroughDecoder
            implements VectorTypeDecoder
    {
        private final ValueVector vector;

        public PassThroughDecoder(ValueVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return vector.getObject(position);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class VarcharDecoder
            implements VectorTypeDecoder
    {
        private final VarCharVector vector;

        public VarcharDecoder(VarCharVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return vector.getObject(position).toString();
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class MapDecoder
            implements VectorTypeDecoder
    {
        private final VectorTypeDecoder keyDecoder;
        private final VectorTypeDecoder valueDecoder;
        private final MapVector vector;

        public MapDecoder(ClientTypeSignature signature, MapVector vector)
        {
            requireNonNull(signature, "signature is null");
            this.vector = requireNonNull(vector, "vector is null");
            StructVector structVector = (StructVector) vector.getDataVector();

            checkArgument(signature.getRawType().equals(MAP), "not a map type signature: %s", signature);
            this.keyDecoder = createVectorTypeDecoder(signature.getArgumentsAsTypeSignatures().get(0), structVector.getChild("key"));
            this.valueDecoder = createVectorTypeDecoder(signature.getArgumentsAsTypeSignatures().get(1), structVector.getChild("value"));
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            if (vector.isEmpty(position)) {
                return Map.of();
            }

            Map<Object, Object> values = new HashMap<>();
            for (int i = vector.getElementStartIndex(position); i < vector.getElementEndIndex(position); i++) {
                values.put(keyDecoder.decode(i), valueDecoder.decode(i));
            }
            return unmodifiableMap(values);
        }

        @Override
        public void close()
                throws IOException
        {
            keyDecoder.close();
            valueDecoder.close();
            vector.close();
        }
    }

    private static class ArrayDecoder
            implements VectorTypeDecoder
    {
        private final VectorTypeDecoder valueDecoder;
        private final ListVector vector;

        public ArrayDecoder(ClientTypeSignature signature, ListVector vector)
        {
            requireNonNull(signature, "signature is null");
            this.vector = requireNonNull(vector, "vector is null");
            checkArgument(signature.getRawType().equals(ARRAY), "not an array type signature: %s", signature);
            this.valueDecoder = createVectorTypeDecoder(signature.getArgumentsAsTypeSignatures().get(0), vector.getDataVector());
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            if (vector.isEmpty(position)) {
                return List.of();
            }

            List<Object> values = new ArrayList<>();
            for (int i = vector.getElementStartIndex(position); i < vector.getElementEndIndex(position); i++) {
                values.add(valueDecoder.decode(i));
            }
            return unmodifiableList(values);
        }

        @Override
        public void close()
                throws IOException
        {
            valueDecoder.close();
            vector.close();
        }
    }

    private static class BigintDecoder
            implements VectorTypeDecoder
    {
        private final BigIntVector vector;

        public BigintDecoder(BigIntVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return vector.get(position);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class DateDecoder
            implements VectorTypeDecoder
    {
        private final DateDayVector vector;

        public DateDecoder(DateDayVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return LocalDate.ofEpochDay(vector.get(position)).toString();
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimestampDecoder
            implements VectorTypeDecoder
    {
        private final TimeStampVector vector;
        private final DateTimeFormatter formatter;

        public TimestampDecoder(TimeStampVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
            this.formatter = timestampFormatter(timestampPrecision(vector));
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return formatter.format(LocalDateTime.ofInstant(toInstant(vector, position), ZoneOffset.UTC));
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class DecimalDecoder
            implements VectorTypeDecoder
    {
        private final DecimalVector vector;

        public DecimalDecoder(DecimalVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            // Same representation as the JSON encoding, which never uses an exponent
            return vector.getObject(position).toPlainString();
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class UuidDecoder
            implements VectorTypeDecoder
    {
        private final FixedSizeBinaryVector vector;

        public UuidDecoder(FixedSizeBinaryVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            // Reconstruct UUID from big-endian byte representation
            byte[] bytes = vector.get(position);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            return new UUID(buffer.getLong(), buffer.getLong()).toString();
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class IntervalDayTimeDecoder
            implements VectorTypeDecoder
    {
        private static final long MICROS_PER_MILLI = 1000;

        private final DurationVector vector;
        // Empty when the client does not support parametric intervals, the value is then rendered in the legacy millisecond form
        private final OptionalInt fractionalPrecision;

        public IntervalDayTimeDecoder(ClientTypeSignature signature, DurationVector vector)
        {
            requireNonNull(signature, "signature is null");
            this.vector = requireNonNull(vector, "vector is null");
            checkArgument(vector.getUnit() == MICROSECOND, "Expected interval encoded as microseconds, but got %s", vector.getUnit());
            this.fractionalPrecision = intervalFractionalPrecision(signature);
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            long micros = DurationVector.get(vector.getDataBuffer(), position);
            if (fractionalPrecision.isPresent()) {
                return formatMicros(micros, fractionalPrecision.orElseThrow());
            }
            // Same rounding as the legacy wire format of the JSON encoding: half up, ties toward positive infinity
            long millis = floorDiv(micros, MICROS_PER_MILLI);
            if (floorMod(micros, MICROS_PER_MILLI) >= MICROS_PER_MILLI / 2) {
                millis++;
            }
            return formatMillis(millis);
        }

        @Override
        public void close()
        {
            vector.close();
        }

        private static OptionalInt intervalFractionalPrecision(ClientTypeSignature signature)
        {
            List<ClientTypeSignatureParameter> arguments = signature.getArguments();
            if (arguments.isEmpty()) {
                return OptionalInt.empty();
            }
            // Parametric interval: start field, end field, leading precision and fractional-seconds precision
            if (arguments.size() < 4) {
                return OptionalInt.of(0);
            }
            return OptionalInt.of(arguments.get(3).getLongLiteral().intValue());
        }
    }

    private static class IntervalYearMonthDecoder
            implements VectorTypeDecoder
    {
        private final IntervalMonthDayNanoVector vector;

        public IntervalYearMonthDecoder(IntervalMonthDayNanoVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            // Year-month intervals are encoded as a number of months in a month-day-nano interval
            ArrowBuf data = vector.getDataBuffer();
            int days = IntervalMonthDayNanoVector.getDays(data, position);
            long nanoseconds = IntervalMonthDayNanoVector.getNanoseconds(data, position);
            checkState(days == 0 && nanoseconds == 0, "Unexpected days or nanoseconds in a year-month interval: %s", vector.getObject(position));
            return formatMonths(IntervalMonthDayNanoVector.getMonths(data, position));
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimeDecoder
            implements VectorTypeDecoder
    {
        // Times with time zone are normalized to UTC during encoding, as Arrow has no time with time zone type
        private static final String UTC_OFFSET = "+00:00";

        private final ValueVector vector;
        private final DateTimeFormatter formatter;
        private final boolean withTimeZone;

        public TimeDecoder(ValueVector vector, boolean withTimeZone)
        {
            this.vector = requireNonNull(vector, "vector is null");
            this.formatter = timeFormatter(timePrecision(vector));
            this.withTimeZone = withTimeZone;
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            long nanoOfDay;
            if (vector instanceof TimeSecVector) {
                nanoOfDay = ((TimeSecVector) vector).get(position) * NANOSECONDS_PER_SECOND;
            }
            else if (vector instanceof TimeMilliVector) {
                nanoOfDay = ((TimeMilliVector) vector).get(position) * NANOSECONDS_PER_MILLISECOND;
            }
            else if (vector instanceof TimeMicroVector) {
                nanoOfDay = ((TimeMicroVector) vector).get(position) * NANOSECONDS_PER_MICROSECOND;
            }
            else {
                nanoOfDay = ((TimeNanoVector) vector).get(position);
            }

            String time = formatter.format(LocalTime.ofNanoOfDay(nanoOfDay));
            if (withTimeZone) {
                return time + UTC_OFFSET;
            }
            return time;
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class TimestampWithTimeZoneDecoder
            implements VectorTypeDecoder
    {
        // Timestamps with time zone are normalized to UTC during encoding, as an Arrow vector has a single time zone
        private static final ZoneId UTC = ZoneId.of("UTC");

        private final TimeStampVector vector;
        private final DateTimeFormatter formatter;

        public TimestampWithTimeZoneDecoder(TimeStampVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
            this.formatter = new DateTimeFormatterBuilder()
                    .append(timestampFormatter(timestampPrecision(vector)))
                    .appendLiteral(' ')
                    .appendZoneId()
                    .toFormatter();
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            return formatter.format(ZonedDateTime.ofInstant(toInstant(vector, position), UTC));
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static class RowDecoder
            implements VectorTypeDecoder
    {
        private final List<VectorTypeDecoder> fieldDecoders;
        private final List<Optional<String>> fieldNames;
        private final StructVector vector;

        public RowDecoder(ClientTypeSignature signature, StructVector vector)
        {
            requireNonNull(signature, "signature is null");
            this.vector = requireNonNull(vector, "vector is null");

            checkArgument(signature.getRawType().equals(ROW), "not a row type signature: %s", signature);
            List<ClientTypeSignatureParameter> arguments = signature.getArguments();
            List<FieldVector> children = vector.getChildrenFromFields();
            checkArgument(children.size() == arguments.size(), "Expected %s row fields, but got %s", arguments.size(), children.size());

            // Field names come from the type signature, as with the JSON encoding; anonymous fields are unnamed
            ImmutableList.Builder<VectorTypeDecoder> fieldDecoders = ImmutableList.builderWithExpectedSize(arguments.size());
            ImmutableList.Builder<Optional<String>> fieldNames = ImmutableList.builderWithExpectedSize(arguments.size());
            for (int i = 0; i < arguments.size(); i++) {
                ClientTypeSignatureParameter parameter = arguments.get(i);
                fieldDecoders.add(createVectorTypeDecoder(parameter.getTypeSignature(), children.get(i)));
                fieldNames.add(parameter.getName());
            }
            this.fieldDecoders = fieldDecoders.build();
            this.fieldNames = fieldNames.build();
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }

            Row.Builder row = Row.builderWithExpectedSize(fieldDecoders.size());
            for (int i = 0; i < fieldDecoders.size(); i++) {
                row.addField(fieldNames.get(i), fieldDecoders.get(i).decode(position));
            }
            return row.build();
        }

        @Override
        public void close()
                throws IOException
        {
            for (VectorTypeDecoder fieldDecoder : fieldDecoders) {
                fieldDecoder.close();
            }
            vector.close();
        }
    }

    private static class JsonDecoder
            implements VectorTypeDecoder
    {
        private final VarCharVector vector;

        public JsonDecoder(VarCharVector vector)
        {
            this.vector = requireNonNull(vector, "vector is null");
        }

        @Override
        public Object decode(int position)
        {
            if (vector.isNull(position)) {
                return null;
            }
            // Return the raw UTF-8 string instead of parsed JSON
            return new String(vector.get(position), StandardCharsets.UTF_8);
        }

        @Override
        public void close()
        {
            vector.close();
        }
    }

    private static int timePrecision(ValueVector vector)
    {
        if (vector instanceof TimeSecVector) {
            return 0;
        }
        if (vector instanceof TimeMilliVector) {
            return 3;
        }
        if (vector instanceof TimeMicroVector) {
            return 6;
        }
        if (vector instanceof TimeNanoVector) {
            return 9;
        }
        throw new IllegalArgumentException("Unsupported time vector type: " + vector.getClass());
    }

    private static int timestampPrecision(TimeStampVector vector)
    {
        if (vector instanceof TimeStampSecVector || vector instanceof TimeStampSecTZVector) {
            return 0;
        }
        if (vector instanceof TimeStampMilliVector || vector instanceof TimeStampMilliTZVector) {
            return 3;
        }
        if (vector instanceof TimeStampMicroVector || vector instanceof TimeStampMicroTZVector) {
            return 6;
        }
        if (vector instanceof TimeStampNanoVector || vector instanceof TimeStampNanoTZVector) {
            return 9;
        }
        throw new IllegalArgumentException("Unsupported timestamp vector type: " + vector.getClass());
    }

    private static Instant toInstant(TimeStampVector vector, int position)
    {
        long value = vector.get(position);
        switch (timestampPrecision(vector)) {
            case 0:
                return Instant.ofEpochSecond(value);
            case 3:
                return Instant.ofEpochMilli(value);
            case 6:
                return ofEpochMicros(value);
            default:
                return ofEpochNanos(value);
        }
    }

    // Same formats as the JSON encoding: the fractional second has exactly as many digits as the precision
    private static DateTimeFormatter timeFormatter(int precision)
    {
        return withFraction(new DateTimeFormatterBuilder().appendPattern("HH:mm:ss"), precision);
    }

    private static DateTimeFormatter timestampFormatter(int precision)
    {
        return withFraction(new DateTimeFormatterBuilder().appendPattern("uuuu-MM-dd HH:mm:ss"), precision);
    }

    private static DateTimeFormatter withFraction(DateTimeFormatterBuilder builder, int precision)
    {
        if (precision > 0) {
            builder.appendFraction(NANO_OF_SECOND, precision, precision, true);
        }
        return builder.toFormatter();
    }

    private static Instant ofEpochMicros(long epochMicros)
    {
        return Instant.ofEpochSecond(floorDiv(epochMicros, MICROSECONDS_PER_SECOND), floorMod(epochMicros, MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND);
    }

    private static Instant ofEpochNanos(long epochNanos)
    {
        return Instant.ofEpochSecond(floorDiv(epochNanos, NANOSECONDS_PER_SECOND), floorMod(epochNanos, NANOSECONDS_PER_SECOND));
    }

    public interface VectorTypeDecoder
            extends Closeable
    {
        Object decode(int position);
    }

    private static <T extends FieldVector> T checkedCast(ValueVector vector, Class<T> clazz)
    {
        checkArgument(clazz.isInstance(vector), "Expected %s, but got %s", clazz, vector.getClass());
        return clazz.cast(vector);
    }
}
