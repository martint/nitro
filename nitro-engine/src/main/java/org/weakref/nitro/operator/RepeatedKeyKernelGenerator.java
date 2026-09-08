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
package org.weakref.nitro.operator;

import org.weakref.nitro.core.type.RepeatedKeyLayout;
import org.weakref.nitro.jit.InMemoryCompiler;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Generates monomorphic direct-array loops for one provider-described repeated key shape. */
final class RepeatedKeyKernelGenerator
        implements AutoCloseable
{
    record OutputShape(ResolvedRepeatedKeyLayout.Storage storage, int nullSourceCount) {}

    record Shape(RepeatedKeyLayout.Order order, List<OutputShape> outputs)
    {
        Shape
        {
            outputs = List.copyOf(outputs);
        }

        static Shape from(ResolvedRepeatedKeyLayout layout)
        {
            return new Shape(
                    layout.order(),
                    layout.outputs().stream()
                            .map(output -> new OutputShape(output.storage(), output.nullPaths().size()))
                            .toList());
        }
    }

    record Kernel(MethodHandle hash, MethodHandle write, MethodHandle identical, MethodHandle identicalInputs) {}

    private static final String PACKAGE = "org.weakref.nitro.operator";

    private final ConcurrentHashMap<Shape, Kernel> kernels = new ConcurrentHashMap<>();
    private final AtomicInteger nextClassId = new AtomicInteger();
    private boolean closed;

    Kernel generate(ResolvedRepeatedKeyLayout layout)
    {
        return generate(Shape.from(layout));
    }

    Kernel generate(Shape shape)
    {
        if (closed) {
            throw new IllegalStateException("Repeated key kernel generator is closed");
        }
        return kernels.computeIfAbsent(shape, this::compile);
    }

    @Override
    public void close()
    {
        closed = true;
        kernels.clear();
    }

    private Kernel compile(Shape shape)
    {
        String simpleName = "GeneratedRepeatedKeyKernel" + nextClassId.incrementAndGet();
        byte[] bytes = InMemoryCompiler.compileToBytes(PACKAGE + "." + simpleName, render(simpleName, shape));
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup().defineHiddenClass(
                    bytes,
                    true,
                    MethodHandles.Lookup.ClassOption.NESTMATE);
            Class<?> generated = lookup.lookupClass();
            return new Kernel(
                    lookup.findStatic(generated, "hash", MethodType.methodType(long.class, RepeatedKeyBatchBinding.class, int.class)),
                    lookup.findStatic(generated, "write", MethodType.methodType(
                            void.class,
                            RepeatedKeyBatchBinding.class,
                            int.class,
                            byte[].class,
                            int.class,
                            FlatGroupingTable.FlatVariableWidthArena.class)),
                    lookup.findStatic(generated, "identical", MethodType.methodType(
                            boolean.class,
                            RepeatedKeyBatchBinding.class,
                            byte[].class,
                            int.class,
                            FlatGroupingTable.FlatVariableWidthArena.class,
                            int.class)),
                    lookup.findStatic(generated, "identicalInputs", MethodType.methodType(
                            boolean.class,
                            RepeatedKeyBatchBinding.class,
                            int.class,
                            int.class)));
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to link generated repeated-key kernel", e);
        }
    }

    private static String render(String simpleName, Shape shape)
    {
        StringBuilder out = new StringBuilder();
        out.append("package ").append(PACKAGE).append(";\n")
                .append("public final class ").append(simpleName).append(" {\n")
                .append("  private static final long NULL_HASH = 0x9E3779B9L;\n")
                .append("  private static int physical(int[] mapping, int mappingOffset, int baseOffset, int position) { return (mapping == null ? position : mapping[position + mappingOffset]) + baseOffset; }\n")
                .append("  private static long mix64(long value) { value ^= value >>> 33; value *= 0xff51afd7ed558ccdL; value ^= value >>> 33; value *= 0xc4ceb9fe1a85ec53L; return value ^ (value >>> 33); }\n")
                .append("  private static void putInt(byte[] target, int offset, int value) { target[offset] = (byte) value; target[offset + 1] = (byte) (value >>> 8); target[offset + 2] = (byte) (value >>> 16); target[offset + 3] = (byte) (value >>> 24); }\n")
                .append("  private static int getInt(byte[] source, int offset) { return (source[offset] & 255) | ((source[offset + 1] & 255) << 8) | ((source[offset + 2] & 255) << 16) | (source[offset + 3] << 24); }\n")
                .append("  private static void putLong(byte[] target, int offset, long value) { putInt(target, offset, (int) value); putInt(target, offset + 4, (int) (value >>> 32)); }\n")
                .append("  private static long getLong(byte[] source, int offset) { return Integer.toUnsignedLong(getInt(source, offset)) | ((long) getInt(source, offset + 4) << 32); }\n");
        appendHash(out, shape);
        appendInputIdentical(out, shape);
        if (shape.order() == RepeatedKeyLayout.Order.ORDERED) {
            appendOrderedWrite(out, shape);
            appendOrderedIdentical(out, shape);
        }
        else {
            appendCanonicalize(out, shape);
            appendUnorderedWrite(out);
            appendUnorderedIdentical(out);
        }
        return out.append("}\n").toString();
    }

    private static void appendInputIdentical(StringBuilder out, Shape shape)
    {
        out.append("  public static boolean identicalInputs(RepeatedKeyBatchBinding b, int left, int right) {\n")
                .append("    int leftParent = physical(b.repeatedMapping, b.repeatedMappingOffset, b.repeatedBaseOffset, left);\n")
                .append("    int rightParent = physical(b.repeatedMapping, b.repeatedMappingOffset, b.repeatedBaseOffset, right);\n")
                .append("    if (leftParent == rightParent) return true;\n");
        if (shape.order() != RepeatedKeyLayout.Order.ORDERED) {
            // Distinct physical unordered values require canonicalization. The authoritative table remains the
            // fallback until a two-input canonical comparator is generated; physical identity is always exact.
            out.append("    return false;\n  }\n");
            return;
        }
        out.append("    int leftStart = b.repeatedOffsets[leftParent]; int leftEnd = b.repeatedOffsets[leftParent + 1];\n")
                .append("    int rightStart = b.repeatedOffsets[rightParent]; int rightEnd = b.repeatedOffsets[rightParent + 1];\n")
                .append("    if (leftEnd - leftStart != rightEnd - rightStart) return false;\n");
        appendBindings(out, shape, "    ");
        out.append("    for (int leftEntry = leftStart, rightEntry = rightStart; leftEntry < leftEnd; leftEntry++, rightEntry++) {\n");
        for (int output = 0; output < shape.outputs().size(); output++) {
            ResolvedRepeatedKeyLayout.Storage storage = shape.outputs().get(output).storage();
            out.append("      boolean leftNull").append(output).append(" = ")
                    .append(nullExpression(shape, output, "leftEntry")).append(";\n")
                    .append("      boolean rightNull").append(output).append(" = ")
                    .append(nullExpression(shape, output, "rightEntry")).append(";\n")
                    .append("      if (leftNull").append(output).append(" != rightNull").append(output).append(") return false;\n")
                    .append("      if (!leftNull").append(output).append(") {");
            if (storage != ResolvedRepeatedKeyLayout.Storage.PRESENCE) {
                out.append(" int leftPosition").append(output).append(" = ")
                        .append(positionExpression(output, "leftEntry")).append(";")
                        .append(" int rightPosition").append(output).append(" = ")
                        .append(positionExpression(output, "rightEntry")).append(";");
            }
            switch (storage) {
                case PRESENCE -> {}
                case I32, I64 -> out.append(" if (v").append(output).append("[leftPosition").append(output)
                        .append("] != v").append(output).append("[rightPosition").append(output).append("]) return false;");
                case BOOLEAN -> out.append(" if (v").append(output).append("[leftPosition").append(output)
                        .append("] != v").append(output).append("[rightPosition").append(output).append("]) return false;");
                case F64 -> out.append(" if (Double.doubleToLongBits(v").append(output).append("[leftPosition").append(output)
                        .append("]) != Double.doubleToLongBits(v").append(output).append("[rightPosition").append(output)
                        .append("])) return false;");
                case BINARY -> out.append(" int leftLength").append(output).append(" = o").append(output)
                        .append("[leftPosition").append(output).append(" + 1] - o").append(output).append("[leftPosition")
                        .append(output).append("]; int rightLength").append(output).append(" = o").append(output)
                        .append("[rightPosition").append(output).append(" + 1] - o").append(output).append("[rightPosition")
                        .append(output).append("]; if (leftLength").append(output).append(" != rightLength").append(output)
                        .append(" || !java.util.Arrays.equals(v").append(output).append(", o").append(output)
                        .append("[leftPosition").append(output).append("], o").append(output).append("[leftPosition")
                        .append(output).append("] + leftLength").append(output).append(", v").append(output).append(", o")
                        .append(output).append("[rightPosition").append(output).append("], o").append(output)
                        .append("[rightPosition").append(output).append("] + rightLength").append(output)
                        .append(")) return false;");
            }
            out.append(" }\n");
        }
        out.append("    }\n    return true;\n  }\n");
    }

    private static void appendHash(StringBuilder out, Shape shape)
    {
        out.append("  public static long hash(RepeatedKeyBatchBinding b, int position) {\n")
                .append("    int parent = physical(b.repeatedMapping, b.repeatedMappingOffset, b.repeatedBaseOffset, position);\n")
                .append("    int start = b.repeatedOffsets[parent]; int end = b.repeatedOffsets[parent + 1];\n");
        appendBindings(out, shape, "    ");
        out.append(shape.order() == RepeatedKeyLayout.Order.ORDERED ? "    long hash = 1;\n" : "    long hash = end - start;\n")
                .append("    for (int entry = start; entry < end; entry++) {\n")
                .append("      long entryHash = 1;\n");
        for (int output = 0; output < shape.outputs().size(); output++) {
            out.append("      long valueHash").append(output).append(";\n")
                    .append("      if (").append(nullExpression(shape, output)).append(") { valueHash").append(output).append(" = NULL_HASH; } else {\n");
            if (shape.outputs().get(output).storage() == ResolvedRepeatedKeyLayout.Storage.PRESENCE) {
                out.append("        valueHash").append(output).append(" = 0;\n");
            }
            else {
                out.append("        int p").append(output).append(" = ").append(positionExpression(output)).append(";\n")
                        .append("        valueHash").append(output).append(" = ").append(hashExpression(shape.outputs().get(output).storage(), output)).append(";\n");
            }
            out.append("      }\n")
                    .append("      entryHash = 31 * entryHash + valueHash").append(output).append(";\n");
        }
        if (shape.order() == RepeatedKeyLayout.Order.ORDERED) {
            out.append("      hash = 31 * hash + entryHash;\n");
        }
        else {
            out.append("      hash += mix64(entryHash);\n");
        }
        out.append("    }\n")
                .append(shape.order() == RepeatedKeyLayout.Order.ORDERED ? "    return hash;\n" : "    return mix64(hash);\n")
                .append("  }\n");
    }

    private static void appendOrderedWrite(StringBuilder out, Shape shape)
    {
        out.append("  public static void write(RepeatedKeyBatchBinding b, int position, byte[] fixed, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena) {\n")
                .append("    int parent = physical(b.repeatedMapping, b.repeatedMappingOffset, b.repeatedBaseOffset, position);\n")
                .append("    int start = b.repeatedOffsets[parent]; int count = b.repeatedOffsets[parent + 1] - start;\n");
        appendBindings(out, shape, "    ");
        out.append("    int length = 4;\n")
                .append("    for (int entry = start; entry < start + count; entry++) {\n")
                .append("      int size = ").append(shape.outputs().size()).append(";\n");
        appendEntrySize(out, shape, "      ");
        out.append("      length = Math.addExact(length, size);\n")
                .append("    }\n")
                .append("    long pointer = arena.reserve(length); int chunkIndex = FlatGroupingTable.FlatVariableWidthArena.chunkIndex(pointer); int chunkOffset = FlatGroupingTable.FlatVariableWidthArena.chunkOffset(pointer);\n")
                .append("    byte[] target = arena.chunk(chunkIndex); putInt(target, chunkOffset, count); int cursor = chunkOffset + 4;\n")
                .append("    for (int entry = start; entry < start + count; entry++) {\n");
        appendWriteEntry(out, shape, "      ", "target", "cursor");
        out.append("    }\n")
                .append("    putInt(fixed, fixedOffset, chunkIndex); putInt(fixed, fixedOffset + 4, chunkOffset); putInt(fixed, fixedOffset + 8, length);\n")
                .append("  }\n");
    }

    private static void appendOrderedIdentical(StringBuilder out, Shape shape)
    {
        out.append("  public static boolean identical(RepeatedKeyBatchBinding b, byte[] fixed, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena, int position) {\n")
                .append("    int parent = physical(b.repeatedMapping, b.repeatedMappingOffset, b.repeatedBaseOffset, position);\n")
                .append("    int start = b.repeatedOffsets[parent]; int count = b.repeatedOffsets[parent + 1] - start;\n")
                .append("    int length = getInt(fixed, fixedOffset + 8); byte[] stored = arena.chunk(getInt(fixed, fixedOffset)); int cursor = getInt(fixed, fixedOffset + 4); int limit = cursor + length;\n")
                .append("    if (length < 4 || getInt(stored, cursor) != count) return false; cursor += 4;\n");
        appendBindings(out, shape, "    ");
        out.append("    for (int entry = start; entry < start + count; entry++) {\n");
        appendCompareEntry(out, shape, "      ");
        out.append("    }\n")
                .append("    return cursor == limit;\n")
                .append("  }\n");
    }

    private static void appendCanonicalize(StringBuilder out, Shape shape)
    {
        out.append("  private static int canonicalize(RepeatedKeyBatchBinding b, int start, int count) {\n");
        appendBindings(out, shape, "    ");
        out.append("    int[] entryOffsets = b.canonicalOffsets(count + 1); entryOffsets[0] = 0; int total = 0;\n")
                .append("    for (int index = 0; index < count; index++) { int entry = start + index; int size = ").append(shape.outputs().size()).append(";\n");
        appendEntrySize(out, shape, "      ");
        out.append("      total = Math.addExact(total, size); entryOffsets[index + 1] = total;\n")
                .append("    }\n")
                .append("    byte[] data = b.canonicalBytes(total); int[] order = b.orderScratch(count); int cursor = 0;\n")
                .append("    for (int index = 0; index < count; index++) { int entry = start + index; order[index] = index;\n");
        appendWriteEntry(out, shape, "      ", "data", "cursor");
        out.append("    }\n")
                .append("    RepeatedKeyCanonicalizer.sort(data, entryOffsets, order, count); return total;\n")
                .append("  }\n");
    }

    private static void appendUnorderedWrite(StringBuilder out)
    {
        out.append("  public static void write(RepeatedKeyBatchBinding b, int position, byte[] fixed, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena) {\n")
                .append("    int parent = physical(b.repeatedMapping, b.repeatedMappingOffset, b.repeatedBaseOffset, position); int start = b.repeatedOffsets[parent]; int count = b.repeatedOffsets[parent + 1] - start;\n")
                .append("    int total = canonicalize(b, start, count); int length = Math.addExact(4, total); long pointer = arena.reserve(length); int chunkIndex = FlatGroupingTable.FlatVariableWidthArena.chunkIndex(pointer); int chunkOffset = FlatGroupingTable.FlatVariableWidthArena.chunkOffset(pointer); byte[] target = arena.chunk(chunkIndex); putInt(target, chunkOffset, count); int cursor = chunkOffset + 4;\n")
                .append("    for (int index = 0; index < count; index++) { int entry = b.orderScratch[index]; int source = b.canonicalOffsets[entry]; int entryLength = b.canonicalOffsets[entry + 1] - source; System.arraycopy(b.canonicalBytes, source, target, cursor, entryLength); cursor += entryLength; }\n")
                .append("    putInt(fixed, fixedOffset, chunkIndex); putInt(fixed, fixedOffset + 4, chunkOffset); putInt(fixed, fixedOffset + 8, length);\n")
                .append("  }\n");
    }

    private static void appendUnorderedIdentical(StringBuilder out)
    {
        out.append("  public static boolean identical(RepeatedKeyBatchBinding b, byte[] fixed, int fixedOffset, FlatGroupingTable.FlatVariableWidthArena arena, int position) {\n")
                .append("    int parent = physical(b.repeatedMapping, b.repeatedMappingOffset, b.repeatedBaseOffset, position); int start = b.repeatedOffsets[parent]; int count = b.repeatedOffsets[parent + 1] - start; int total = canonicalize(b, start, count);\n")
                .append("    int length = getInt(fixed, fixedOffset + 8); byte[] stored = arena.chunk(getInt(fixed, fixedOffset)); int cursor = getInt(fixed, fixedOffset + 4); if (length != total + 4 || getInt(stored, cursor) != count) return false; cursor += 4;\n")
                .append("    for (int index = 0; index < count; index++) { int entry = b.orderScratch[index]; int source = b.canonicalOffsets[entry]; int entryLength = b.canonicalOffsets[entry + 1] - source; if (!java.util.Arrays.equals(b.canonicalBytes, source, source + entryLength, stored, cursor, cursor + entryLength)) return false; cursor += entryLength; }\n")
                .append("    return true;\n")
                .append("  }\n");
    }

    private static void appendBindings(StringBuilder out, Shape shape, String indent)
    {
        for (int output = 0; output < shape.outputs().size(); output++) {
            ResolvedRepeatedKeyLayout.Storage storage = shape.outputs().get(output).storage();
            String type = switch (storage) {
                case PRESENCE -> null;
                case I32 -> "int[]";
                case I64 -> "long[]";
                case BOOLEAN -> "boolean[]";
                case F64 -> "double[]";
                case BINARY -> "byte[]";
            };
            if (type != null) {
                out.append(indent).append(type).append(" v").append(output).append(" = (").append(type).append(") b.outputArrays[").append(output).append("];\n")
                        .append(indent).append("int[] m").append(output).append(" = b.outputMappings[").append(output).append("]; int mo").append(output).append(" = b.outputMappingOffsets[").append(output).append("]; int mb").append(output).append(" = b.outputBaseOffsets[").append(output).append("];\n");
            }
            for (int source = 0; source < shape.outputs().get(output).nullSourceCount(); source++) {
                out.append(indent).append("boolean[] n").append(output).append('_').append(source).append(" = b.nullArrays[").append(output).append("][").append(source).append("]; int[] nm").append(output).append('_').append(source).append(" = b.nullMappings[").append(output).append("][").append(source).append("]; int nmo").append(output).append('_').append(source).append(" = b.nullMappingOffsets[").append(output).append("][").append(source).append("]; int nb").append(output).append('_').append(source).append(" = b.nullBaseOffsets[").append(output).append("][").append(source).append("];\n");
            }
            if (storage == ResolvedRepeatedKeyLayout.Storage.BINARY) {
                out.append(indent).append("int[] o").append(output).append(" = b.outputBinaryOffsets[").append(output).append("];\n");
            }
        }
    }

    private static void appendEntrySize(StringBuilder out, Shape shape, String indent)
    {
        for (int output = 0; output < shape.outputs().size(); output++) {
            out.append(indent).append("if (!(").append(nullExpression(shape, output)).append(")) { ");
            switch (shape.outputs().get(output).storage()) {
                case PRESENCE -> {}
                case I32, I64, F64 -> out.append("size = Math.addExact(size, 8);");
                case BOOLEAN -> out.append("size = Math.addExact(size, 1);");
                case BINARY -> out.append("int p").append(output).append(" = ").append(positionExpression(output)).append("; size = Math.addExact(size, Math.addExact(4, o").append(output).append("[p").append(output).append(" + 1] - o").append(output).append("[p").append(output).append("]));");
            }
            out.append(" }\n");
        }
    }

    private static void appendWriteEntry(StringBuilder out, Shape shape, String indent, String target, String cursor)
    {
        for (int output = 0; output < shape.outputs().size(); output++) {
            ResolvedRepeatedKeyLayout.Storage storage = shape.outputs().get(output).storage();
            out.append(indent).append("boolean z").append(output).append(" = ").append(nullExpression(shape, output)).append("; ").append(target).append("[").append(cursor).append("++] = (byte) (z").append(output).append(" ? 1 : 0);\n");
            if (storage == ResolvedRepeatedKeyLayout.Storage.PRESENCE) {
                continue;
            }
            out.append(indent).append("if (!z").append(output).append(") { int p").append(output).append(" = ").append(positionExpression(output)).append("; ");
            switch (storage) {
                case PRESENCE -> throw new AssertionError();
                case I32, I64 -> out.append("putLong(").append(target).append(", ").append(cursor).append(", v").append(output).append("[p").append(output).append("]); ").append(cursor).append(" += 8;");
                case BOOLEAN -> out.append(target).append("[").append(cursor).append("++] = (byte) (v").append(output).append("[p").append(output).append("] ? 1 : 0);");
                case F64 -> out.append("putLong(").append(target).append(", ").append(cursor).append(", Double.doubleToLongBits(v").append(output).append("[p").append(output).append("])); ").append(cursor).append(" += 8;");
                case BINARY -> out.append("int length").append(output).append(" = o").append(output).append("[p").append(output).append(" + 1] - o").append(output).append("[p").append(output).append("]; putInt(").append(target).append(", ").append(cursor).append(", length").append(output).append("); ").append(cursor).append(" += 4; System.arraycopy(v").append(output).append(", o").append(output).append("[p").append(output).append("], ").append(target).append(", ").append(cursor).append(", length").append(output).append("); ").append(cursor).append(" += length").append(output).append(";");
            }
            out.append(" }\n");
        }
    }

    private static void appendCompareEntry(StringBuilder out, Shape shape, String indent)
    {
        for (int output = 0; output < shape.outputs().size(); output++) {
            ResolvedRepeatedKeyLayout.Storage storage = shape.outputs().get(output).storage();
            out.append(indent).append("if (cursor >= limit) return false; boolean z").append(output).append(" = ").append(nullExpression(shape, output)).append("; if ((stored[cursor++] != 0) != z").append(output).append(") return false;\n");
            if (storage == ResolvedRepeatedKeyLayout.Storage.PRESENCE) {
                continue;
            }
            out.append(indent).append("if (!z").append(output).append(") { int p").append(output).append(" = ").append(positionExpression(output)).append("; ");
            switch (storage) {
                case PRESENCE -> throw new AssertionError();
                case I32, I64 -> out.append("if (cursor + 8 > limit || getLong(stored, cursor) != v").append(output).append("[p").append(output).append("]) return false; cursor += 8;");
                case BOOLEAN -> out.append("if (cursor >= limit || (stored[cursor++] != 0) != v").append(output).append("[p").append(output).append("]) return false;");
                case F64 -> out.append("if (cursor + 8 > limit || getLong(stored, cursor) != Double.doubleToLongBits(v").append(output).append("[p").append(output).append("])) return false; cursor += 8;");
                case BINARY -> out.append("if (cursor + 4 > limit) return false; int length").append(output).append(" = getInt(stored, cursor); cursor += 4; int sourceLength").append(output).append(" = o").append(output).append("[p").append(output).append(" + 1] - o").append(output).append("[p").append(output).append("]; if (length").append(output).append(" < 0 || cursor + length").append(output).append(" > limit || sourceLength").append(output).append(" != length").append(output).append(" || !java.util.Arrays.equals(v").append(output).append(", o").append(output).append("[p").append(output).append("], o").append(output).append("[p").append(output).append("] + length").append(output).append(", stored, cursor, cursor + length").append(output).append(")) return false; cursor += length").append(output).append(";");
            }
            out.append(" }\n");
        }
    }

    private static String nullExpression(Shape shape, int output)
    {
        return nullExpression(shape, output, "entry");
    }

    private static String nullExpression(Shape shape, int output, String entry)
    {
        StringBuilder expression = new StringBuilder();
        for (int source = 0; source < shape.outputs().get(output).nullSourceCount(); source++) {
            if (!expression.isEmpty()) {
                expression.append(" || ");
            }
            expression.append("n").append(output).append('_').append(source)
                    .append(" != null && n").append(output).append('_').append(source)
                    .append("[physical(nm").append(output).append('_').append(source)
                    .append(", nmo").append(output).append('_').append(source)
                    .append(", nb").append(output).append('_').append(source).append(", ").append(entry).append(")]");
        }
        return expression.isEmpty() ? "false" : "(" + expression + ")";
    }

    private static String positionExpression(int output)
    {
        return positionExpression(output, "entry");
    }

    private static String positionExpression(int output, String entry)
    {
        return "physical(m" + output + ", mo" + output + ", mb" + output + ", " + entry + ")";
    }

    private static String hashExpression(ResolvedRepeatedKeyLayout.Storage storage, int output)
    {
        return switch (storage) {
            case PRESENCE -> "0";
            case I32 -> "Long.hashCode(v" + output + "[p" + output + "])";
            case I64 -> "Long.hashCode(v" + output + "[p" + output + "])";
            case BOOLEAN -> "Boolean.hashCode(v" + output + "[p" + output + "])";
            case F64 -> "Long.hashCode(Double.doubleToLongBits(v" + output + "[p" + output + "]))";
            case BINARY -> "OperatorVectorSupport.binaryHash(v" + output + ", o" + output + "[p" + output + "], o" + output + "[p" + output + " + 1] - o" + output + "[p" + output + "])";
        };
    }
}
