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

import io.trino.spi.block.Block;
import io.trino.type.IntervalYearMonthType;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;

import static java.util.Objects.requireNonNull;

public final class IntervalYearMonthWriter
        extends FixedWidthWriter<IntervalMonthDayNanoVector>
{
    private final IntervalYearMonthType type;

    public IntervalYearMonthWriter(IntervalMonthDayNanoVector vector, IntervalYearMonthType type)
    {
        super(vector);
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    protected void setNull(int position)
    {
        vector.setNull(position);
    }

    @Override
    protected void writeValue(Block block, int position)
    {
        vector.set(position, type.getInt(block, position), 0, 0);
    }
}
