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

import org.weakref.nitro.core.type.LongFlatKeyStorage;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Vector;

import java.util.Optional;
import java.util.Set;

import static java.lang.Math.toIntExact;

final class CanonicalFlatKeyTestType
{
    private CanonicalFlatKeyTestType() {}

    static TypeBinding signedInteger()
    {
        return new TypeBinding()
        {
            @Override
            public TypeIdentity identity()
            {
                return new TypeIdentity("test_signed_integer");
            }

            @Override
            public Class<?> carrierType()
            {
                return long.class;
            }

            @Override
            public TypeOperators operators()
            {
                return TypeOperators.UNSPECIFIED;
            }

            @Override
            public Optional<LongFlatKeyStorage> longFlatKeyStorage()
            {
                return Optional.of(new LongFlatKeyStorage()
                {
                    @Override
                    public int fixedSize()
                    {
                        return Integer.BYTES;
                    }

                    @Override
                    public void write(byte[] target, int offset, long value)
                    {
                        int integer = toIntExact(value);
                        target[offset] = (byte) integer;
                        target[offset + 1] = (byte) (integer >>> 8);
                        target[offset + 2] = (byte) (integer >>> 16);
                        target[offset + 3] = (byte) (integer >>> 24);
                    }

                    @Override
                    public long read(byte[] source, int offset)
                    {
                        return (source[offset] & 0xFF)
                                | (source[offset + 1] & 0xFF) << 8
                                | (source[offset + 2] & 0xFF) << 16
                                | source[offset + 3] << 24;
                    }
                });
            }

            @Override
            public Set<Class<? extends Vector>> supportedVectorTypes()
            {
                return Set.of(I32Vector.class, I64Vector.class);
            }
        };
    }
}
