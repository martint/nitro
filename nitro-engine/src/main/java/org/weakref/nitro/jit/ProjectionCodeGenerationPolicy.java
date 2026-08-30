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
        boolean returnedConstantComparisonMasks,
        int dictionaryEqualityMinimumReuse,
        int fusedDictionaryDomainMinimumReduction)
{
    public ProjectionCodeGenerationPolicy
    {
        if (dictionaryEqualityMinimumReuse < 1) {
            throw new IllegalArgumentException("dictionaryEqualityMinimumReuse must be at least one");
        }
        if (fusedDictionaryDomainMinimumReduction < 1) {
            throw new IllegalArgumentException("fusedDictionaryDomainMinimumReduction must be at least one");
        }
    }

    public static ProjectionCodeGenerationPolicy defaults()
    {
        return new ProjectionCodeGenerationPolicy(true, true, true, 4, 4);
    }

    public static ProjectionCodeGenerationPolicy fromSystemProperties()
    {
        ProjectionCodeGenerationPolicy defaults = defaults();
        return new ProjectionCodeGenerationPolicy(
                booleanProperty("nitro.project.pooledDictionaryScratch", defaults.pooledDictionaryScratch()),
                booleanProperty("nitro.project.mappedDictionaryDoubleInputs", defaults.mappedDictionaryDoubleInputs()),
                booleanProperty("nitro.longComparison.returnedConstantMasks", defaults.returnedConstantComparisonMasks()),
                integerProperty("nitro.utf8.dictionaryEqualityMinimumReuse", defaults.dictionaryEqualityMinimumReuse()),
                integerProperty("nitro.project.fusedDictionaryDomainMinimumReduction", defaults.fusedDictionaryDomainMinimumReduction()));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }

    private static int integerProperty(String name, int defaultValue)
    {
        return Integer.parseInt(System.getProperty(name, Integer.toString(defaultValue)));
    }
}
