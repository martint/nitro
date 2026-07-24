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

import org.junit.jupiter.api.Test;
import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.DictionaryVector;
import org.weakref.nitro.data.EngineResources;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Vector;
import org.weakref.nitro.operator.evaluator.ir.Stream;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestBatchBufferScope
{
    @Test
    void returnsUntakenBuffersForNextGeneration()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        BatchBufferScope scope = new BatchBufferScope(allocator, "test");
        I64Vector first = allocator.allocate(scope.context(), I64Vector.class, 32, I64Vector::new);
        Batch batch = scope.batch(
                allocator.allocateAllMask(scope.context(), 32),
                new Output(Set.of(Stream.VALUES), _ -> first, scope));

        batch.output(0).borrow(Stream.VALUES);
        batch.close();

        I64Vector second = allocator.allocate(scope.context(), I64Vector.class, 32, I64Vector::new);
        assertThat(second).isSameAs(first);
    }

    @Test
    void takenBufferIsDetachedFromReusePool()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        BatchBufferScope scope = new BatchBufferScope(allocator, "test");
        I64Vector first = allocator.allocate(scope.context(), I64Vector.class, 32, I64Vector::new);
        Batch batch = scope.batch(
                allocator.allocateAllMask(scope.context(), 32),
                new Output(Set.of(Stream.VALUES), _ -> first, scope));

        assertThat(batch.output(0).take(Stream.VALUES)).isSameAs(first);
        batch.close();

        I64Vector second = allocator.allocate(scope.context(), I64Vector.class, 32, I64Vector::new);
        assertThat(second).isNotSameAs(first);
    }

    @Test
    void takingEncodedVectorDoesNotDetachBorrowedChild()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context upstream = new Allocator.Context("upstream");
        I64Vector borrowedChild = allocator.allocate(upstream, I64Vector.class, 4, I64Vector::new);
        BatchBufferScope scope = new BatchBufferScope(allocator, "test");
        DictionaryVector encoded = allocator.allocateDictionary(scope.context(), new int[] {0, 1, 2, 3}, borrowedChild);
        Batch batch = scope.batch(
                allocator.allocateAllMask(scope.context(), 4),
                new Output(Set.of(Stream.VALUES), _ -> encoded, scope));

        Vector taken = batch.output(0).take(Stream.VALUES);
        batch.close();

        assertThat(taken).isSameAs(encoded);
        assertThat(allocator.currentBytes(upstream)).isGreaterThan(0);
        allocator.release(upstream, borrowedChild);
    }

    @Test
    void ownedDictionaryIdsFollowTheEncodingLifecycleWithoutClaimingBorrowedValues()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context upstream = new Allocator.Context("upstream");
        I64Vector borrowedValues = allocator.allocate(upstream, I64Vector.class, 4, I64Vector::new);
        BatchBufferScope scope = new BatchBufferScope(allocator, "dictionary");
        I32Vector ids = allocator.allocate(scope.context(), I32Vector.class, 4, I32Vector::new);
        ids.values()[0] = 3;
        ids.values()[1] = 2;
        ids.values()[2] = 1;
        ids.values()[3] = 0;
        DictionaryVector encoded = DictionaryVector.wrapOwnedIds(ids, 4, borrowedValues);
        Batch batch = scope.batch(
                allocator.allocateAllMask(scope.context(), 4),
                new Output(Set.of(Stream.VALUES), _ -> encoded, scope));

        batch.output(0).borrow(Stream.VALUES);
        batch.close();

        I32Vector reusedIds = allocator.allocate(scope.context(), I32Vector.class, 4, I32Vector::new);
        assertThat(reusedIds).isSameAs(ids);
        assertThat(allocator.currentBytes(upstream)).isGreaterThan(0);
        allocator.release(upstream, borrowedValues);
    }

    @Test
    void takenDictionaryIdsReturnWhenTheFinalForwardedOutputCloses()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        BatchBufferScope scope = new BatchBufferScope(allocator, "dictionary");
        I32Vector ids = allocator.allocate(scope.context(), I32Vector.class, 4, I32Vector::new);
        DictionaryVector encoded = DictionaryVector.wrapOwnedIds(ids, 4, new I64Vector(4));
        Batch sourceBatch = scope.batch(
                allocator.allocateAllMask(scope.context(), 4),
                new Output(Set.of(Stream.VALUES), _ -> encoded, scope));

        Vector taken = sourceBatch.output(0).take(Stream.VALUES);
        sourceBatch.close();
        I32Vector whileTaken = allocator.allocate(scope.context(), I32Vector.class, 4, I32Vector::new);
        assertThat(whileTaken).isNotSameAs(ids);

        Output forwarded = Output.of(Streams.ofValues(taken));
        forwarded.borrow(Stream.VALUES);
        forwarded.close();

        I32Vector afterClose = allocator.allocate(scope.context(), I32Vector.class, 4, I32Vector::new);
        assertThat(afterClose).isSameAs(ids);
        allocator.release(scope.context(), whileTaken);
        allocator.release(scope.context(), afterClose);
    }

    @Test
    void borrowedNestedEncodingDoesNotReleaseItsChildLease()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        BatchBufferScope scope = new BatchBufferScope(allocator, "dictionary");
        I32Vector ids = allocator.allocate(scope.context(), I32Vector.class, 4, I32Vector::new);
        DictionaryVector encoded = DictionaryVector.wrapOwnedIds(ids, 4, new I64Vector(4));
        Batch sourceBatch = scope.batch(
                allocator.allocateAllMask(scope.context(), 4),
                new Output(Set.of(Stream.VALUES), _ -> encoded, scope));
        Vector taken = sourceBatch.output(0).take(Stream.VALUES);
        sourceBatch.close();

        Output borrowedWrapper = Output.of(Streams.ofValues(DictionaryVector.wrapNested(new int[] {0, 1, 2, 3}, 4, taken)));
        borrowedWrapper.borrow(Stream.VALUES);
        borrowedWrapper.close();
        I32Vector whileChildIsRetained = allocator.allocate(scope.context(), I32Vector.class, 4, I32Vector::new);
        assertThat(whileChildIsRetained).isNotSameAs(ids);

        Output finalOwner = Output.of(Streams.ofValues(taken));
        finalOwner.borrow(Stream.VALUES);
        finalOwner.close();
        I32Vector afterOwnerClose = allocator.allocate(scope.context(), I32Vector.class, 4, I32Vector::new);
        assertThat(afterOwnerClose).isSameAs(ids);
        allocator.release(scope.context(), whileChildIsRetained);
        allocator.release(scope.context(), afterOwnerClose);
    }

    @Test
    void constrainedForeignMaskKeepsItsOwner()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context upstream = new Allocator.Context("upstream");
        Mask foreign = allocator.allocateSparseMask(upstream, new int[] {1, 3, 5}, 10);
        BatchBufferScope scope = new BatchBufferScope(allocator, "test");
        Batch batch = scope.batch(allocator.allocateAllMask(scope.context(), 10));

        batch.constrain(foreign);
        assertThat(batch.takeMask()).isSameAs(foreign);
        batch.close();

        assertThat(allocator.currentBytes(upstream)).isGreaterThan(0);
        allocator.release(upstream, foreign);
    }

    @Test
    void staleFacadesStayClosedAndOverlappingGenerationsAreRejected()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        BatchBufferScope scope = new BatchBufferScope(allocator, "test");
        I64Vector vector = allocator.allocate(scope.context(), I64Vector.class, 1, I64Vector::new);
        Output output = new Output(Set.of(Stream.VALUES), _ -> vector, scope);
        Batch first = scope.batch(allocator.allocateAllMask(scope.context(), 1), output);

        assertThatThrownBy(() -> scope.batch(allocator.allocateAllMask(scope.context(), 1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("still open");
        first.close();
        Batch second = scope.batch(allocator.allocateAllMask(scope.context(), 1));

        assertThatThrownBy(() -> first.borrowMask()).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> output.borrow(Stream.VALUES)).isInstanceOf(IllegalStateException.class);
        second.close();
    }

    @Test
    void compatibleScopesSharePoolsWithoutSharingOwnership()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Object poolGroup = new Object();
        BatchBufferScope firstScope = new BatchBufferScope(allocator, "first", poolGroup);
        BatchBufferScope secondScope = new BatchBufferScope(allocator, "second", poolGroup);
        I64Vector firstVector = allocator.allocate(firstScope.context(), I64Vector.class, 32, I64Vector::new);
        I64Vector secondVector = allocator.allocate(secondScope.context(), I64Vector.class, 32, I64Vector::new);
        Batch first = firstScope.batch(allocator.allocateAllMask(firstScope.context(), 32));
        Batch second = secondScope.batch(allocator.allocateAllMask(secondScope.context(), 32));

        first.close();
        assertThat(allocator.currentBytes(new Allocator.Context("second"))).isGreaterThan(0);
        second.close();

        BatchBufferScope thirdScope = new BatchBufferScope(allocator, "third", poolGroup);
        I64Vector reused = allocator.allocate(thirdScope.context(), I64Vector.class, 32, I64Vector::new);
        assertThat(reused).isIn(firstVector, secondVector);
    }

    @Test
    void composesAdditionalGenerationOwnerWithoutPerOutputCallbacks()
    {
        Allocator allocator = new Allocator(EngineResources.createDefault());
        Allocator.Context evaluatorContext = new Allocator.Context("evaluator");
        I64Vector vector = allocator.allocate(evaluatorContext, I64Vector.class, 8, I64Vector::new);
        int[] calls = new int[3];
        BatchBufferOwner evaluatorOwner = new BatchBufferOwner()
        {
            @Override
            public Vector take(Vector owned)
            {
                calls[0]++;
                return allocator.transferOwned(evaluatorContext, owned);
            }

            @Override
            public void release(Vector owned)
            {
                calls[1]++;
                allocator.release(evaluatorContext, owned);
            }

            @Override
            public void releaseAll()
            {
                calls[2]++;
                allocator.release(evaluatorContext);
            }
        };
        BatchBufferScope scope = new BatchBufferScope(allocator, "operator");
        scope.begin(evaluatorOwner);
        Output output = new Output(Set.of(Stream.VALUES), _ -> vector, scope);

        output.borrow(Stream.VALUES);
        output.close();
        scope.endBatch();

        assertThat(calls).containsExactly(0, 1, 1);
    }
}
