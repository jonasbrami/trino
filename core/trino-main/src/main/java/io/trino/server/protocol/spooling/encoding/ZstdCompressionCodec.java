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
package io.trino.server.protocol.spooling.encoding;

import io.airlift.compress.v3.zstd.ZstdCompressor;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

import java.lang.foreign.MemorySegment;

import static java.lang.Math.toIntExact;
import static org.apache.arrow.vector.compression.CompressionUtil.CodecType.ZSTD;
import static org.apache.arrow.vector.compression.CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH;

public class ZstdCompressionCodec
        extends AbstractCompressionCodec
{
    // Zstd compressors are stateless, and a codec is only used by the stream writer that created it
    private final ZstdCompressor compressor = ZstdCompressor.create();

    @Override
    protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer)
    {
        long maxCompressedLength = compressor.maxCompressedLength(toIntExact(uncompressedBuffer.writerIndex()));
        // The compressed data follows the uncompressed length, which AbstractCompressionCodec writes afterward
        ArrowBuf compressedBuffer = allocator.buffer(SIZE_OF_UNCOMPRESSED_LENGTH + maxCompressedLength);

        try {
            long written = compressor.compress(
                    MemorySegment.ofAddress(uncompressedBuffer.memoryAddress()).reinterpret(uncompressedBuffer.writerIndex()),
                    MemorySegment.ofAddress(compressedBuffer.memoryAddress() + SIZE_OF_UNCOMPRESSED_LENGTH).reinterpret(maxCompressedLength));
            compressedBuffer.writerIndex(SIZE_OF_UNCOMPRESSED_LENGTH + written);
            return compressedBuffer;
        }
        catch (Exception e) {
            compressedBuffer.close();
            throw new RuntimeException("Error compressing to ZSTD", e);
        }
    }

    @Override
    protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer)
    {
        throw new UnsupportedOperationException("Decompression is not supported server-side");
    }

    @Override
    public CompressionUtil.CodecType getCodecType()
    {
        return ZSTD;
    }
}
