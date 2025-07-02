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
package org.weakref.nitro.operator.evaluator;

import org.weakref.nitro.operator.evaluator.ir.Assignment;
import org.weakref.nitro.operator.evaluator.ir.Variable;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class Evaluator
{
//    private final Map<Variable, MemoEntry> memo = new HashMap<>();
//
//    private final List<Variable> outputs;
//    private final List<Assignment> program;
//
    private final Map<Variable, Assignment> assignments;

//    private final InputProvider inputs;
//    private final Allocator allocator;
//
    public Evaluator(
            List<Assignment> program,
            Allocator allocator)
    {
//        this.program = program;
//        this.outputs = outputs;
//        this.inputs = inputs;
//        this.allocator = allocator;
        this.assignments = program.stream().collect(toImmutableMap(Assignment::variable, Function.identity()));
    }
//
//    public Vector evaluate(Variable variable, Mask mask)
//    {
//        Vector result;
//        Mask remaining = mask;
//
//        MemoEntry entry = memo.get(variable);
//        if (entry != null) {
//            remaining = mask.difference(entry.mask());
//            if (remaining.none()) {
//                return entry.vector();
//            }
//            result = entry.vector();
//        }
//        else {
//            Assignment assignment = assignments.get(variable);
//            if (assignment == null) {
//                result = inputs.getInput(variable, mask);
//                memo.put(variable, new MemoEntry(result, mask));
//                return result;
//            }
//            result = allocator.allocate(assignment.type());
//        }
//
//        // Evaluate the assignment for the missing mask
//        Assignment assignment = assignments.get(variable);
//        if (assignment == null) {
//            // Input variable: fetch from inputProvider (should not reach here)
//            result = inputs.getInput(variable, remaining);
//            memo.put(variable, new MemoEntry(result, mask.copy()));
//            return result;
//        }
//
//        // Evaluate the operation for the positions in toComputeMask
//        try {
//            evaluateOperation(assignment, result, remaining);
//        }
//        catch (Exception e) {
//            // Mark errors in the vector for the positions in toComputeMask
//            result.setErrors(remaining, true);
//        }
//
//        // Merge masks: previously computed + just computed
//        Mask newMask;
//        if (entry != null) {
//            newMask = entry.mask().union(remaining);
//        }
//        else {
//            newMask = remaining.copy();
//        }
//        memo.put(variable, new MemoEntry(result, newMask));
//        return result;
//    }
//
//    public void reset()
//    {
//        // Return vectors to allocator and clear memo
//        for (MemoEntry entry : memo.values()) {
//            allocator.release(entry.vector());
//            // Optionally: release entry.mask() if masks are pooled
//        }
//        memo.clear();
//    }
//
//    private record MemoEntry(Vector vector, Mask mask) { }
}
