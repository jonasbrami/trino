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
package io.trino.server.protocol.spooling.encoding.arrow;

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestamp;
import org.apache.arrow.vector.TimeStampNanoVector;

import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;

public final class TimestampNanoWriter
        extends FixedWidthWriter<TimeStampNanoVector>
{
    static final String NANOSECOND_RANGE = "1677-09-21 00:12:43.145224192 and 2262-04-11 23:47:16.854775807 UTC";

    public TimestampNanoWriter(TimeStampNanoVector vector)
    {
        super(vector);
    }

    @Override
    protected void setNull(int position)
    {
        vector.setNull(position);
    }

    @Override
    protected void writeValue(Block block, int position)
    {
        LongTimestamp timestamp = (LongTimestamp) TIMESTAMP_NANOS.getObject(block, position);
        try {
            vector.set(position, toEpochNanos(timestamp.getEpochMicros(), NANOSECONDS_PER_MICROSECOND, timestamp.getPicosOfMicro() / PICOSECONDS_PER_NANOSECOND));
        }
        catch (ArithmeticException e) {
            throw new TrinoException(NUMERIC_VALUE_OUT_OF_RANGE, "Timestamp %s is out of range for the Arrow encoding, which supports values between %s".formatted(TIMESTAMP_NANOS.getObjectValue(block, position), NANOSECOND_RANGE), e);
        }
    }

    /**
     * Returns {@code epochUnits * nanosecondsPerUnit + nanosecondsOfUnit}, avoiding the overflow of the product for the
     * smallest values that are still representable.
     *
     * @throws ArithmeticException if the value is out of the range of nanoseconds since the epoch
     */
    static long toEpochNanos(long epochUnits, long nanosecondsPerUnit, long nanosecondsOfUnit)
    {
        if (epochUnits < 0) {
            return addExact(multiplyExact(epochUnits + 1, nanosecondsPerUnit), nanosecondsOfUnit - nanosecondsPerUnit);
        }
        return addExact(multiplyExact(epochUnits, nanosecondsPerUnit), nanosecondsOfUnit);
    }
}
