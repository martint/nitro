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
package org.weakref.nitro.data;

import static java.util.Objects.requireNonNull;

/**
 * Immutable policy for allocator representation and local/compatible retention behavior.
 */
public record AllocatorPolicy(
        boolean directSingleRunRle,
        boolean reuseTransportTuples,
        boolean transferableBufferLeases,
        BooleanCopies booleanCopies,
        MaskFiltering maskFiltering,
        int maxPooledMasksPerBucket,
        boolean complementDifferenceMasks,
        boolean singleCopySparseMasks,
        boolean localVectorWorkingSet,
        boolean adaptiveVectorPoolHighWater,
        long maxLocalVectorPoolBytes,
        long maxCompatibleVectorPoolBytes,
        AggregateStateVectorRetention aggregateStateVectorRetention,
        int compatibleVectorPoolRetentionMultiplier,
        int compatiblePoolLocalReserve,
        int minCompatibilityDomainGroups,
        int compatibleMaskPoolLocalReserve,
        boolean fastVectorPoolOrderRemove,
        boolean sharedAllFalseBoolean,
        boolean directVectorFactory,
        boolean indexedVectorTreeTraversal,
        int maxVariableWidthStorageOversizeRatio)
{
    public AllocatorPolicy
    {
        requireNonNull(booleanCopies, "booleanCopies is null");
        requireNonNull(maskFiltering, "maskFiltering is null");
        requireNonNull(aggregateStateVectorRetention, "aggregateStateVectorRetention is null");
        if (maxVariableWidthStorageOversizeRatio < 1) {
            throw new IllegalArgumentException("maxVariableWidthStorageOversizeRatio is less than 1");
        }
    }

    public static AllocatorPolicy defaults()
    {
        return new AllocatorPolicy(
                true,
                true,
                true,
                BooleanCopies.defaults(),
                MaskFiltering.defaults(),
                4,
                false,
                true,
                true,
                true,
                64L << 20,
                64L << 20,
                AggregateStateVectorRetention.defaults(),
                1,
                0,
                3,
                0,
                true,
                true,
                true,
                true,
                8);
    }

    public static AllocatorPolicy fromSystemProperties()
    {
        AllocatorPolicy defaults = defaults();
        long maxLocalVectorPoolBytes =
                Long.getLong("nitro.allocator.maxLocalVectorPoolBytes", defaults.maxLocalVectorPoolBytes());
        int compatiblePoolLocalReserve =
                Integer.getInteger("nitro.allocator.compatiblePoolLocalReserve", defaults.compatiblePoolLocalReserve());
        return new AllocatorPolicy(
                booleanProperty("nitro.directSingleRunRle", defaults.directSingleRunRle()),
                !Boolean.getBoolean("nitro.streams.disableTransportTupleReuse"),
                booleanProperty("nitro.transferableBufferLeases", defaults.transferableBufferLeases()),
                BooleanCopies.fromSystemProperties(),
                MaskFiltering.fromSystemProperties(),
                defaults.maxPooledMasksPerBucket(),
                booleanProperty("nitro.mask.complementDifferenceMasks", defaults.complementDifferenceMasks()),
                booleanProperty("nitro.mask.singleCopySparseMasks", defaults.singleCopySparseMasks()),
                booleanProperty("nitro.allocator.localVectorWorkingSet", defaults.localVectorWorkingSet()),
                booleanProperty("nitro.allocator.adaptiveVectorPoolHighWater", defaults.adaptiveVectorPoolHighWater()),
                maxLocalVectorPoolBytes,
                Long.getLong("nitro.allocator.maxCompatibleVectorPoolBytes", maxLocalVectorPoolBytes),
                AggregateStateVectorRetention.fromSystemProperties(),
                Integer.getInteger(
                        "nitro.allocator.compatibleVectorPoolRetentionMultiplier",
                        defaults.compatibleVectorPoolRetentionMultiplier()),
                compatiblePoolLocalReserve,
                Integer.getInteger(
                        "nitro.allocator.minCompatibilityDomainGroups",
                        defaults.minCompatibilityDomainGroups()),
                Integer.getInteger("nitro.allocator.compatibleMaskPoolLocalReserve", compatiblePoolLocalReserve),
                booleanProperty("nitro.allocator.fastVectorPoolOrderRemove", defaults.fastVectorPoolOrderRemove()),
                booleanProperty("nitro.allocator.sharedAllFalseBoolean", defaults.sharedAllFalseBoolean()),
                booleanProperty("nitro.allocator.directVectorFactory", defaults.directVectorFactory()),
                booleanProperty("nitro.allocator.indexedVectorTreeTraversal", defaults.indexedVectorTreeTraversal()),
                Integer.getInteger(
                        "nitro.allocator.maxVariableWidthStorageOversizeRatio",
                        defaults.maxVariableWidthStorageOversizeRatio()));
    }

    public AllocatorPolicy withAggregateStateVectorRetention(AggregateStateVectorRetention retention)
    {
        return new AllocatorPolicy(
                directSingleRunRle,
                reuseTransportTuples,
                transferableBufferLeases,
                booleanCopies,
                maskFiltering,
                maxPooledMasksPerBucket,
                complementDifferenceMasks,
                singleCopySparseMasks,
                localVectorWorkingSet,
                adaptiveVectorPoolHighWater,
                maxLocalVectorPoolBytes,
                maxCompatibleVectorPoolBytes,
                requireNonNull(retention, "retention is null"),
                compatibleVectorPoolRetentionMultiplier,
                compatiblePoolLocalReserve,
                minCompatibilityDomainGroups,
                compatibleMaskPoolLocalReserve,
                fastVectorPoolOrderRemove,
                sharedAllFalseBoolean,
                directVectorFactory,
                indexedVectorTreeTraversal,
                maxVariableWidthStorageOversizeRatio);
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }

    public record BooleanCopies(
            boolean directDense,
            boolean monotonicConcatenatedPositions,
            boolean directConcatenatedPositions)
    {
        public static BooleanCopies defaults()
        {
            return new BooleanCopies(true, true, true);
        }

        private static BooleanCopies fromSystemProperties()
        {
            BooleanCopies defaults = defaults();
            return new BooleanCopies(
                    booleanProperty("nitro.scalar.directDenseIsNullBooleanCopy", defaults.directDense()),
                    booleanProperty(
                            "nitro.concatenatedBoolean.monotonicPositionCopy",
                            defaults.monotonicConcatenatedPositions()),
                    booleanProperty(
                            "nitro.concatenatedBoolean.directPositionCopy",
                            defaults.directConcatenatedPositions()));
        }
    }

    public record MaskFiltering(
            boolean vectorizedDenseIntConstantRange,
            boolean branchlessDenseDoubleLessThan,
            double dictionarySparseWriteMaximumSelectedDomainFraction)
    {
        public MaskFiltering
        {
            if (dictionarySparseWriteMaximumSelectedDomainFraction < 0 || dictionarySparseWriteMaximumSelectedDomainFraction > 1) {
                throw new IllegalArgumentException("dictionarySparseWriteMaximumSelectedDomainFraction is outside [0, 1]");
            }
        }

        public static MaskFiltering defaults()
        {
            return new MaskFiltering(true, true, 0.5);
        }

        private static MaskFiltering fromSystemProperties()
        {
            MaskFiltering defaults = defaults();
            return new MaskFiltering(
                    booleanProperty(
                            "nitro.mask.vectorizedDenseIntConstantRange",
                            defaults.vectorizedDenseIntConstantRange()),
                    booleanProperty(
                            "nitro.mask.branchlessDenseDoubleLessThan",
                            defaults.branchlessDenseDoubleLessThan()),
                    Double.parseDouble(System.getProperty(
                            "nitro.mask.dictionarySparseWriteMaximumSelectedDomainFraction",
                            Double.toString(defaults.dictionarySparseWriteMaximumSelectedDomainFraction()))));
        }
    }

    public record AggregateStateVectorRetention(
            int maxRetainedPerBucket,
            long maxRetainedBytes,
            long maxWideRetainedBytes)
    {
        public AggregateStateVectorRetention
        {
            if (maxRetainedPerBucket < 0) {
                throw new IllegalArgumentException("maxRetainedPerBucket is negative");
            }
            if (maxRetainedBytes < 0) {
                throw new IllegalArgumentException("maxRetainedBytes is negative");
            }
            if (maxWideRetainedBytes < 0) {
                throw new IllegalArgumentException("maxWideRetainedBytes is negative");
            }
        }

        public static AggregateStateVectorRetention defaults()
        {
            return new AggregateStateVectorRetention(2, 8L << 20, 16L << 20);
        }

        private static AggregateStateVectorRetention fromSystemProperties()
        {
            AggregateStateVectorRetention defaults = defaults();
            return new AggregateStateVectorRetention(
                    Integer.getInteger(
                            "nitro.allocator.aggregateState.maxRetainedPerBucket",
                            defaults.maxRetainedPerBucket()),
                    Long.getLong(
                            "nitro.allocator.aggregateState.maxRetainedBytes",
                            defaults.maxRetainedBytes()),
                    Long.getLong(
                            "nitro.allocator.aggregateState.maxWideRetainedBytes",
                            defaults.maxWideRetainedBytes()));
        }

        public int maxRetained(VectorPoolRetentionClass retentionClass, long retainedBytes)
        {
            long maxBytes = switch (retentionClass) {
                case AGGREGATE_STATE -> maxRetainedBytes;
                case WIDE_AGGREGATE_STATE -> maxWideRetainedBytes;
                case VECTOR_DEFAULT -> throw new IllegalArgumentException("VECTOR_DEFAULT has no aggregate-state retention");
            };
            return retainedBytes <= maxBytes ? maxRetainedPerBucket : 0;
        }
    }
}
