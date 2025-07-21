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

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.arrow.shaded.arrow.vector.BigIntVector;
import io.trino.arrow.shaded.arrow.vector.BitVector;
import io.trino.arrow.shaded.arrow.vector.DateDayVector;
import io.trino.arrow.shaded.arrow.vector.DecimalVector;
import io.trino.arrow.shaded.arrow.vector.FixedSizeBinaryVector;
import io.trino.arrow.shaded.arrow.vector.Float4Vector;
import io.trino.arrow.shaded.arrow.vector.Float8Vector;
import io.trino.arrow.shaded.arrow.vector.IntVector;
import io.trino.arrow.shaded.arrow.vector.IntervalDayVector;
import io.trino.arrow.shaded.arrow.vector.NullVector;
import io.trino.arrow.shaded.arrow.vector.SmallIntVector;
import io.trino.arrow.shaded.arrow.vector.TimeMicroVector;
import io.trino.arrow.shaded.arrow.vector.TimeMilliVector;
import io.trino.arrow.shaded.arrow.vector.TimeNanoVector;
import io.trino.arrow.shaded.arrow.vector.TimeSecVector;
import io.trino.arrow.shaded.arrow.vector.TimeStampMicroVector;
import io.trino.arrow.shaded.arrow.vector.TimeStampMilliVector;
import io.trino.arrow.shaded.arrow.vector.TimeStampNanoVector;
import io.trino.arrow.shaded.arrow.vector.TimeStampSecVector;
import io.trino.arrow.shaded.arrow.vector.TinyIntVector;
import io.trino.arrow.shaded.arrow.vector.ValueVector;
import io.trino.arrow.shaded.arrow.vector.VarBinaryVector;
import io.trino.arrow.shaded.arrow.vector.VarCharVector;
import io.trino.arrow.shaded.arrow.vector.complex.ListVector;
import io.trino.arrow.shaded.arrow.vector.complex.MapVector;
import io.trino.arrow.shaded.arrow.vector.complex.StructVector;

public final class VectorWriters
{
    private VectorWriters() {}

    public static ArrowWriter writerForVector(ValueVector valueVector, Type type)
    {
        return switch (valueVector) {
            case BitVector vector -> new BooleanWriter(vector);
            case TinyIntVector vector -> new TinyIntWriter(vector);
            case SmallIntVector vector -> new SmallIntWriter(vector);
            case IntVector vector -> new IntegerWriter(vector);
            case BigIntVector vector -> new BigintWriter(vector);
            case Float4Vector vector -> new RealWriter(vector);
            case Float8Vector vector -> new DoubleWriter(vector);
            case VarCharVector vector -> type instanceof CharType charType ? new CharWriter(vector, charType) : new VarcharWriter(vector);
            case VarBinaryVector vector -> new VarbinaryWriter(vector);
            case DateDayVector vector -> new DateWriter(vector);
            case DecimalVector vector -> new DecimalWriter(vector, (DecimalType) type);
            case IntervalDayVector vector -> new IntervalDayWriter(vector);
            case FixedSizeBinaryVector vector -> new UuidWriter(vector);
            case TimeSecVector vector -> new TimeSecWriter(vector);
            case TimeMilliVector vector -> new TimeMilliWriter(vector);
            case TimeMicroVector vector -> new TimeMicroWriter(vector);
            case TimeNanoVector vector -> new TimeNanoWriter(vector);
            case TimeStampSecVector vector -> new TimestampSecWriter(vector);
            case TimeStampMilliVector vector -> new TimestampMilliWriter(vector);
            case TimeStampMicroVector vector -> new TimestampMicroWriter(vector);
            case TimeStampNanoVector vector -> new TimestampNanoWriter(vector);
            case MapVector vector -> new MapWriter(vector, (MapType) type);
            case ListVector vector -> new ArrayWriter(vector, (ArrayType) type);
            case StructVector vector -> new RowWriter(vector, (RowType) type);
            case NullVector vector -> new NullWriter(vector);
            default -> throw new UnsupportedOperationException("Unsupported vector type: " + valueVector.getClass().getName());
        };
    }
}
