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
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.jit.CompiledPipeline;
import org.weakref.nitro.operator.Batch;
import org.weakref.nitro.operator.Operator;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Validates that the data-centric compiler produces byte-identical results to the interpreted operator tree
 * for a real TPC-DS star-schema query over real sf10 Parquet (see {@link CompiledQuerySupport}). Skipped when
 * the dataset is not configured.
 */
public class TestCompiledQuery
{
    @Test
    void compiledMatchesInterpretedOnStoreSalesByItem()
    {
        TpcdsParquetTables tables = TpcdsParquetTables.actualIfPresent("sf10").orElse(null);
        assumeTrue(tables != null, "Set -D" + TpcdsParquetTables.TPCDS_PARQUET_PATH_PROPERTY + "=/path/to/tpcds-parquet-sf10");

        CompiledQuerySupport.Loaded data = CompiledQuerySupport.load(new Allocator(), tables);

        Map<Long, Long> interpreted = runInterpreted(data);
        Map<Long, Long> compiled = runCompiled(data);

        assertThat(compiled).isEqualTo(interpreted);
        assertThat(compiled).isNotEmpty();
    }

    private static Map<Long, Long> runInterpreted(CompiledQuerySupport.Loaded data)
    {
        Map<Long, Long> result = new HashMap<>();
        try (Operator aggregation = CompiledQuerySupport.interpreted(new Allocator(), data)) {
            while (aggregation.hasNext()) {
                try (Batch batch = aggregation.next()) {
                    Mask mask = batch.borrowMask();
                    I64Vector keys = (I64Vector) batch.output(0).borrow(Stream.VALUES);
                    I64Vector sums = (I64Vector) batch.output(1).borrow(Stream.VALUES);
                    for (int index = 0; index < mask.count(); index++) {
                        int position = mask.position(index);
                        result.put(keys.values()[position], sums.values()[position]);
                    }
                }
            }
        }
        return result;
    }

    private static Map<Long, Long> runCompiled(CompiledQuerySupport.Loaded data)
    {
        CompiledPipeline.Result result = CompiledQuerySupport.runCompiled(CompiledQuerySupport.compile(), data);
        long[] keys = result.columns()[0];
        long[] sums = result.columns()[1];
        Map<Long, Long> map = new HashMap<>();
        for (int g = 0; g < result.rowCount(); g++) {
            map.put(keys[g], sums[g]);
        }
        return map;
    }
}
