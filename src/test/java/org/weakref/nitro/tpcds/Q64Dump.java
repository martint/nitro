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
package org.weakref.nitro.tpcds;

import org.junit.jupiter.api.Test;

class Q64Dump
{
    @Test
    void dump()
            throws Exception
    {
        CompiledTpcdsQueries.Composite composite = CompiledTpcdsQueries.query64();
        CompiledTpcdsQueries.Stage cross = composite.stages().get(1);
        org.weakref.nitro.legacy.pipeline.QueryLowering.Lowered lowered = cross.plan().lower();
        java.nio.file.Files.writeString(java.nio.file.Path.of("/tmp/q64_cross.java"),
                new org.weakref.nitro.legacy.pipeline.PipelineCompiler(org.weakref.nitro.legacy.pipeline.CompilerResources.createDefault()).renderStreaming(lowered.pipeline(), lowered.encodings(), lowered.nullable()));
    }
}
