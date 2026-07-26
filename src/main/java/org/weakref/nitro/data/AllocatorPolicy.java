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

/**
 * Immutable policy for allocator representation and local/compatible retention behavior.
 */
public record AllocatorPolicy(
        boolean directSingleRunRle,
        int maxPooledMasksPerBucket,
        boolean complementDifferenceMasks,
        boolean singleCopySparseMasks,
        boolean localVectorWorkingSet,
        boolean adaptiveVectorPoolHighWater,
        long maxLocalVectorPoolBytes,
        long maxCompatibleVectorPoolBytes,
        int compatibleVectorPoolRetentionMultiplier,
        int compatiblePoolLocalReserve,
        int minCompatibilityDomainGroups,
        int compatibleMaskPoolLocalReserve,
        boolean fastVectorPoolOrderRemove,
        boolean sharedAllFalseBoolean,
        boolean directVectorFactory,
        boolean indexedVectorTreeTraversal)
{
    public static AllocatorPolicy defaults()
    {
        return new AllocatorPolicy(
                true,
                4,
                false,
                true,
                true,
                true,
                64L << 20,
                64L << 20,
                1,
                0,
                3,
                0,
                true,
                true,
                true,
                true);
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
                defaults.maxPooledMasksPerBucket(),
                booleanProperty("nitro.mask.complementDifferenceMasks", defaults.complementDifferenceMasks()),
                booleanProperty("nitro.mask.singleCopySparseMasks", defaults.singleCopySparseMasks()),
                booleanProperty("nitro.allocator.localVectorWorkingSet", defaults.localVectorWorkingSet()),
                booleanProperty("nitro.allocator.adaptiveVectorPoolHighWater", defaults.adaptiveVectorPoolHighWater()),
                maxLocalVectorPoolBytes,
                Long.getLong("nitro.allocator.maxCompatibleVectorPoolBytes", maxLocalVectorPoolBytes),
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
                booleanProperty("nitro.allocator.indexedVectorTreeTraversal", defaults.indexedVectorTreeTraversal()));
    }

    private static boolean booleanProperty(String name, boolean defaultValue)
    {
        return Boolean.parseBoolean(System.getProperty(name, Boolean.toString(defaultValue)));
    }
}
