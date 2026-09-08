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

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

final class RepeatedKeyKernelBootstrap
{
    private RepeatedKeyKernelBootstrap() {}

    static CallSite bootstrap(MethodHandles.Lookup lookup, String name, MethodType type)
            throws IllegalAccessException
    {
        ProjectedFlatKeyClassData data = MethodHandles.classData(lookup, "_", ProjectedFlatKeyClassData.class);
        int separator = name.indexOf('_');
        String operation = name.substring(0, separator);
        int index = Integer.parseInt(name, separator + 1, name.length(), 10);
        RepeatedKeyKernelGenerator.Kernel kernel = data.repeatedKernels().get(index);
        MethodHandle target = switch (operation) {
            case "hash" -> kernel.hash();
            case "write" -> kernel.write();
            case "identical" -> kernel.identical();
            case "identicalInputs" -> kernel.identicalInputs();
            default -> throw new IllegalArgumentException("Unknown repeated-key operation: " + operation);
        };
        return new ConstantCallSite(target.asType(type));
    }
}
