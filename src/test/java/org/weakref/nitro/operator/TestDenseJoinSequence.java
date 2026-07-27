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

import static org.assertj.core.api.Assertions.assertThat;

class TestDenseJoinSequence
{
    @Test
    void observesConsecutiveKeysAndSingleBatchReferences()
    {
        DenseJoinSequence sequence = new DenseJoinSequence(true, true);
        long base = JoinRowReference.pack(3, 7);

        assertThat(sequence.acceptsKey(11, 0)).isTrue();
        sequence.observeDenseRow(11, base, 0);
        assertThat(sequence.acceptsKey(12, 1)).isTrue();
        sequence.observeDenseRow(12, base + 1, 1);

        assertThat(sequence.keyCandidate()).isTrue();
        assertThat(sequence.referenceCandidate()).isTrue();
        assertThat(sequence.firstKey()).isEqualTo(11);
        sequence.activateObservedReferences();
        assertThat(sequence.referencesActive()).isTrue();
        assertThat(sequence.referenceBatchIndex()).isEqualTo(3);
        assertThat(sequence.firstReferencePosition()).isEqualTo(7);
        assertThat(sequence.referenceAt(1)).isEqualTo(base + 1);
    }

    @Test
    void rejectsBothCandidatesAfterDenseBuildDiverges()
    {
        DenseJoinSequence sequence = new DenseJoinSequence(true, true);
        sequence.observeDenseRow(11, JoinRowReference.pack(3, 7), 0);

        assertThat(sequence.acceptsKey(13, 1)).isFalse();
        sequence.reject();

        assertThat(sequence.keyCandidate()).isFalse();
        assertThat(sequence.referenceCandidate()).isFalse();
    }

    @Test
    void rejectsOnlyReferenceCandidateWhenReferenceSequenceDiverges()
    {
        DenseJoinSequence sequence = new DenseJoinSequence(true, true);
        sequence.observeDenseRow(11, JoinRowReference.pack(3, 7), 0);
        sequence.observeDenseRow(12, JoinRowReference.pack(4, 8), 1);

        assertThat(sequence.keyCandidate()).isTrue();
        assertThat(sequence.referenceCandidate()).isFalse();
    }

    @Test
    void directBuildCanDisableKeyDetectionAndActivateExplicitReferences()
    {
        DenseJoinSequence sequence = new DenseJoinSequence(true, true);
        long base = JoinRowReference.pack(5, 9);

        sequence.disableKeyCandidate();
        sequence.activateReferences(5, 9, base);

        assertThat(sequence.keyCandidate()).isFalse();
        assertThat(sequence.referenceCandidate()).isTrue();
        assertThat(sequence.referencesActive()).isTrue();
        assertThat(sequence.referenceBase()).isEqualTo(base);
    }
}
