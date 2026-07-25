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
package org.weakref.nitro.jit;

import java.lang.classfile.ClassFile;
import java.lang.classfile.Label;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import static java.lang.constant.ConstantDescs.CD_boolean;
import static java.lang.constant.ConstantDescs.CD_int;
import static java.lang.constant.ConstantDescs.CD_void;

final class Utf8LiteralMatcherGenerator
{
    private static final ClassDesc CD_OBJECT = ClassDesc.of("java.lang.Object");
    private static final ClassDesc CD_MATCHER = ClassDesc.of("org.weakref.nitro.jit.Utf8LiteralMatcher");
    private static final ClassDesc CD_BYTE_ARRAY = ClassDesc.ofDescriptor("[B");
    private static final MethodTypeDesc MATCH_TYPE = MethodTypeDesc.of(CD_boolean, CD_BYTE_ARRAY, CD_int, CD_int);

    private Utf8LiteralMatcherGenerator() {}

    static Utf8LiteralMatcher generate(byte[] literal)
    {
        ClassDesc generatedClass = ClassDesc.of("org.weakref.nitro.jit.GeneratedUtf8LiteralMatcher");
        byte[] classBytes = ClassFile.of().build(generatedClass, builder -> {
            builder.withSuperclass(CD_OBJECT);
            builder.withInterfaceSymbols(CD_MATCHER);
            builder.withFlags(ClassFile.ACC_FINAL | ClassFile.ACC_SYNTHETIC);
            builder.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PUBLIC, code -> {
                code.aload(0);
                code.invokespecial(CD_OBJECT, "<init>", MethodTypeDesc.of(CD_void));
                code.return_();
            });
            builder.withMethodBody("matches", MATCH_TYPE, ClassFile.ACC_PUBLIC, code -> {
                Label different = code.newLabel();
                code.iload(3);
                code.loadConstant(literal.length);
                code.if_icmpne(different);
                for (int index = 0; index < literal.length; index++) {
                    code.aload(1);
                    code.iload(2);
                    if (index != 0) {
                        code.loadConstant(index);
                        code.iadd();
                    }
                    code.baload();
                    code.loadConstant((int) literal[index]);
                    code.if_icmpne(different);
                }
                code.loadConstant(1);
                code.ireturn();
                code.labelBinding(different);
                code.loadConstant(0);
                code.ireturn();
            });
        });

        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup()
                    .defineHiddenClass(classBytes, true, MethodHandles.Lookup.ClassOption.NESTMATE);
            return (Utf8LiteralMatcher) lookup.findConstructor(
                    lookup.lookupClass(),
                    MethodType.methodType(void.class)).invoke();
        }
        catch (Throwable throwable) {
            throw new IllegalStateException("Failed to generate UTF-8 literal matcher", throwable);
        }
    }
}
