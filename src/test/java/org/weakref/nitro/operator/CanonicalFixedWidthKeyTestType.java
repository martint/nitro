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

import org.weakref.nitro.core.type.FixedWidthKeyLayout;
import org.weakref.nitro.core.type.TypeBinding;
import org.weakref.nitro.core.type.TypeIdentity;
import org.weakref.nitro.core.type.TypeOperators;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.RegionVector;
import org.weakref.nitro.data.RleVector;
import org.weakref.nitro.data.Vector;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/** Test provider type whose physical long includes representation-only zone bits. */
final class CanonicalFixedWidthKeyTestType
        implements TypeBinding
{
    private static final int ZONE_BITS = 12;
    private static final long ZONE_MASK = (1L << ZONE_BITS) - 1;
    private static final MethodHandle CANONICAL_INSTANT = canonicalInstantHandle();
    private static final FixedWidthKeyLayout LAYOUT = new FixedWidthKeyLayout(List.of(FixedWidthKeyLayout.Lane.projected(
            List.of(new FixedWidthKeyLayout.Source(List.of(), FixedWidthKeyLayout.Carrier.I64)),
            CANONICAL_INSTANT)));

    static final CanonicalFixedWidthKeyTestType INSTANCE = new CanonicalFixedWidthKeyTestType();

    private CanonicalFixedWidthKeyTestType() {}

    static long pack(long instant, int zone)
    {
        if ((zone & ~ZONE_MASK) != 0) {
            throw new IllegalArgumentException("zone does not fit");
        }
        return (instant << ZONE_BITS) | zone;
    }

    @Override
    public TypeIdentity identity()
    {
        return new TypeIdentity("testing:canonical-packed-instant");
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
    public Optional<FixedWidthKeyLayout> fixedWidthKeyLayout()
    {
        return Optional.of(LAYOUT);
    }

    @Override
    public Set<Class<? extends Vector>> supportedVectorTypes()
    {
        return Set.of(I64Vector.class, DictionaryVector.class, RegionVector.class, RleVector.class);
    }

    private static MethodHandle canonicalInstantHandle()
    {
        try {
            return MethodHandles.lookup().findStatic(
                    CanonicalFixedWidthKeyTestType.class,
                    "canonicalInstant",
                    MethodType.methodType(long.class, long.class));
        }
        catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static long canonicalInstant(long packed)
    {
        return packed >> ZONE_BITS;
    }
}
