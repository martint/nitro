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

/**
 * Immutable engine-owner policy for generated projection and comparison-mask kernels.
 *
 * <p>The property-backed factory is a standalone composition adapter. Compilers retain an explicitly supplied
 * instance and never inspect process configuration.
 */
public record ProjectionCodeGenerationPolicy(
        boolean pooledDictionaryScratch,
        boolean mappedDictionaryDoubleInputs,
        boolean returnedConstantComparisonMasks)
{
    public static ProjectionCodeGenerationPolicy defaults()
    {
        return new ProjectionCodeGenerationPolicy(true, true, true);
    }

    public static ProjectionCodeGenerationPolicy fromSystemProperties()
    {
        ProjectionCodeGenerationPolicy defaults = defaults();
        return new ProjectionCodeGenerationPolicy(
                booleanProperty("nitro.project.pooledDictionaryScratch", defaults.pooledDictionaryScratch()),
                booleanProperty("nitro.project.mappedDictionaryDoubleInputs", defaults.mappedDictionaryDoubleInputs()),
                booleanProperty("nitro.longComparison.returnedConstantMasks", defaults.returnedConstantComparisonMasks()));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
