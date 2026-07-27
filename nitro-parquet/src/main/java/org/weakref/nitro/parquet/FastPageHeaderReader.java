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
package org.weakref.nitro.parquet;

import java.lang.foreign.MemorySegment;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

/**
 * Allocation-free reader for the small subset of Thrift Compact Protocol carried by a Parquet page header.
 * Unknown fields are skipped generically, so optional CRC/statistics fields do not affect the hot reader.
 */
final class FastPageHeaderReader
{
    private static final int STOP = 0;
    private static final int BOOLEAN_TRUE = 1;
    private static final int BOOLEAN_FALSE = 2;
    private static final int BYTE = 3;
    private static final int I16 = 4;
    private static final int I32 = 5;
    private static final int I64 = 6;
    private static final int DOUBLE = 7;
    private static final int BINARY = 8;
    private static final int LIST = 9;
    private static final int SET = 10;
    private static final int MAP = 11;
    private static final int STRUCT = 12;

    private MemorySegment input;
    private long position;
    private long end;

    private int type;
    private int uncompressedSize;
    private int compressedSize;
    private int valueCount;
    private int encoding;

    public long read(MemorySegment input, long offset, long end)
    {
        this.input = input;
        this.position = offset;
        this.end = end;
        type = -1;
        uncompressedSize = -1;
        compressedSize = -1;
        valueCount = -1;
        encoding = -1;

        int fieldId = 0;
        while (true) {
            int field = readUnsignedByte();
            int compactType = field & 0x0F;
            if (compactType == STOP) {
                break;
            }
            int delta = field >>> 4;
            fieldId = delta == 0 ? readI16() : fieldId + delta;
            switch (fieldId) {
                case 1 -> type = readRequiredI32(compactType);
                case 2 -> uncompressedSize = readRequiredI32(compactType);
                case 3 -> compressedSize = readRequiredI32(compactType);
                case 5 -> {
                    requireType(compactType, STRUCT);
                    readDataPageHeader();
                }
                case 7 -> {
                    requireType(compactType, STRUCT);
                    readDictionaryPageHeader();
                }
                default -> skipField(compactType);
            }
        }
        if (type < 0 || uncompressedSize < 0 || compressedSize < 0) {
            throw malformed("missing required PageHeader field");
        }
        return position;
    }

    public int type()
    {
        return type;
    }

    public int uncompressedSize()
    {
        return uncompressedSize;
    }

    public int compressedSize()
    {
        return compressedSize;
    }

    public int valueCount()
    {
        return valueCount;
    }

    public int encoding()
    {
        return encoding;
    }

    private void readDataPageHeader()
    {
        int fieldId = 0;
        while (true) {
            int field = readUnsignedByte();
            int compactType = field & 0x0F;
            if (compactType == STOP) {
                return;
            }
            int delta = field >>> 4;
            fieldId = delta == 0 ? readI16() : fieldId + delta;
            switch (fieldId) {
                case 1 -> valueCount = readRequiredI32(compactType);
                case 2 -> encoding = readRequiredI32(compactType);
                default -> skipField(compactType);
            }
        }
    }

    private void readDictionaryPageHeader()
    {
        int fieldId = 0;
        while (true) {
            int field = readUnsignedByte();
            int compactType = field & 0x0F;
            if (compactType == STOP) {
                return;
            }
            int delta = field >>> 4;
            fieldId = delta == 0 ? readI16() : fieldId + delta;
            switch (fieldId) {
                case 1 -> valueCount = readRequiredI32(compactType);
                case 2 -> encoding = readRequiredI32(compactType);
                default -> skipField(compactType);
            }
        }
    }

    private int readRequiredI32(int compactType)
    {
        requireType(compactType, I32);
        return zigZag(readVarint64());
    }

    private int readI16()
    {
        return zigZag(readVarint64());
    }

    private static int zigZag(long value)
    {
        return (int) ((value >>> 1) ^ -(value & 1));
    }

    private void skipField(int compactType)
    {
        skip(compactType, false);
    }

    private void skip(int compactType, boolean collectionElement)
    {
        switch (compactType) {
            case BOOLEAN_TRUE, BOOLEAN_FALSE -> {
                if (collectionElement) {
                    readUnsignedByte();
                }
            }
            case BYTE -> readUnsignedByte();
            case I16, I32, I64 -> readVarint64();
            case DOUBLE -> advance(8);
            case BINARY -> advance(readUnsignedVarint32());
            case LIST, SET -> {
                int header = readUnsignedByte();
                int size = header >>> 4;
                int elementType = header & 0x0F;
                if (size == 15) {
                    size = readUnsignedVarint32();
                }
                for (int index = 0; index < size; index++) {
                    skip(elementType, true);
                }
            }
            case MAP -> {
                int size = readUnsignedVarint32();
                if (size != 0) {
                    int types = readUnsignedByte();
                    int keyType = types >>> 4;
                    int valueType = types & 0x0F;
                    for (int index = 0; index < size; index++) {
                        skip(keyType, true);
                        skip(valueType, true);
                    }
                }
            }
            case STRUCT -> skipStruct();
            default -> throw malformed("unknown compact type " + compactType);
        }
    }

    private void skipStruct()
    {
        while (true) {
            int field = readUnsignedByte();
            int compactType = field & 0x0F;
            if (compactType == STOP) {
                return;
            }
            if ((field >>> 4) == 0) {
                readI16();
            }
            skipField(compactType);
        }
    }

    private int readUnsignedVarint32()
    {
        long value = readVarint64();
        if ((value & 0xFFFF_FFFF_0000_0000L) != 0) {
            throw malformed("varint32 overflow");
        }
        return (int) value;
    }

    private long readVarint64()
    {
        long value = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            int next = readUnsignedByte();
            value |= (long) (next & 0x7F) << shift;
            if ((next & 0x80) == 0) {
                return value;
            }
        }
        throw malformed("unterminated varint");
    }

    private int readUnsignedByte()
    {
        if (position >= end) {
            throw malformed("truncated input");
        }
        return input.get(JAVA_BYTE, position++) & 0xFF;
    }

    private void advance(long bytes)
    {
        if (bytes < 0 || position + bytes < position || position + bytes > end) {
            throw malformed("truncated input");
        }
        position += bytes;
    }

    private static void requireType(int actual, int expected)
    {
        if (actual != expected) {
            throw new IllegalArgumentException("Malformed Parquet page header: expected compact type " + expected + ", got " + actual);
        }
    }

    private static IllegalArgumentException malformed(String message)
    {
        return new IllegalArgumentException("Malformed Parquet page header: " + message);
    }
}
