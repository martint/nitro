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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Immutable engine-selected policy for global and grouped aggregation execution.
 */
public record AggregationOperatorPolicy(
        boolean deferResultMaterialization,
        int maxOutputBatchRows,
        int fuseGroupLimit,
        int fuseLocalGroupLimit,
        int fuseMappedOnlyGroupLimit,
        boolean dynamicFilterThroughAggregation,
        boolean sparseConstrainedResults,
        boolean groupPartitionedLongDistinct,
        boolean partialGeneratedGrouping,
        boolean fusedDictionaryInput,
        boolean dictionaryDomainAggregation,
        int dictionaryDomainAggregationMinReduction,
        boolean fusedLongRunCache,
        boolean fusedConstantRuns,
        int fusedConstantRunGroupMin,
        boolean fusedLongDirectGrouping,
        boolean fusedMappedContinuationPowerOfTwoStateCapacity,
        boolean debugFusedGrouping)
{
    public AggregationOperatorPolicy
    {
        checkArgument(maxOutputBatchRows > 0, "maxOutputBatchRows must be positive");
        checkArgument(fuseGroupLimit > 0, "fuseGroupLimit must be positive");
        checkArgument(fuseLocalGroupLimit > 0, "fuseLocalGroupLimit must be positive");
        checkArgument(fuseMappedOnlyGroupLimit > 0, "fuseMappedOnlyGroupLimit must be positive");
        checkArgument(dictionaryDomainAggregationMinReduction > 0, "dictionaryDomainAggregationMinReduction must be positive");
        checkArgument(fusedConstantRunGroupMin > 0, "fusedConstantRunGroupMin must be positive");
    }

    public static AggregationOperatorPolicy defaults()
    {
        return new AggregationOperatorPolicy(
                true,
                1 << 14,
                1 << 15,
                1 << 25,
                1 << 16,
                true,
                true,
                true,
                true,
                true,
                true,
                4,
                true,
                true,
                1 << 15,
                true,
                true,
                false);
    }

    /**
     * Reads Nitro's standalone command-line policy. Embedding engines should construct the policy directly.
     */
    public static AggregationOperatorPolicy fromSystemProperties()
    {
        AggregationOperatorPolicy defaults = defaults();
        return new AggregationOperatorPolicy(
                Boolean.parseBoolean(System.getProperty(
                        "nitro.aggregate.deferResultMaterialization",
                        Boolean.toString(defaults.deferResultMaterialization()))),
                Integer.getInteger("nitro.aggregate.maxOutputBatchRows", defaults.maxOutputBatchRows()),
                Integer.getInteger("nitro.groupedAggregation.fuseGroupLimit", defaults.fuseGroupLimit()),
                Integer.getInteger("nitro.groupedAggregation.fuseLocalGroupLimit", defaults.fuseLocalGroupLimit()),
                Integer.getInteger("nitro.groupedAggregation.fuseMappedOnlyGroupLimit", defaults.fuseMappedOnlyGroupLimit()),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.dynamicFilter.throughAggregation",
                        Boolean.toString(defaults.dynamicFilterThroughAggregation()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.groupedAggregation.sparseConstrainedResults",
                        Boolean.toString(defaults.sparseConstrainedResults()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.distinct.groupPartitionedLong",
                        Boolean.toString(defaults.groupPartitionedLongDistinct()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.groupedAggregation.partialGeneratedGrouping",
                        Boolean.toString(defaults.partialGeneratedGrouping()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.groupedAggregation.fusedDictionaryInput",
                        Boolean.toString(defaults.fusedDictionaryInput()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.groupedAggregation.dictionaryDomainAggregation",
                        Boolean.toString(defaults.dictionaryDomainAggregation()))),
                Integer.getInteger(
                        "nitro.groupedAggregation.dictionaryDomainAggregationMinReduction",
                        defaults.dictionaryDomainAggregationMinReduction()),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.groupedAggregation.fusedLongRunCache",
                        Boolean.toString(defaults.fusedLongRunCache()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.groupedAggregation.fusedConstantRuns",
                        Boolean.toString(defaults.fusedConstantRuns()))),
                Integer.getInteger(
                        "nitro.groupedAggregation.fusedConstantRunGroupMin",
                        defaults.fusedConstantRunGroupMin()),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.groupedAggregation.fusedLongDirectGrouping",
                        Boolean.toString(defaults.fusedLongDirectGrouping()))),
                Boolean.parseBoolean(System.getProperty(
                        "nitro.groupedAggregation.fusedMappedContinuationPowerOfTwoStateCapacity",
                        Boolean.toString(defaults.fusedMappedContinuationPowerOfTwoStateCapacity()))),
                Boolean.getBoolean("nitro.debug.fusedGrouping"));
    }
}
