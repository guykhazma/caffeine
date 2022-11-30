package com.github.benmanes.caffeine.cache.simulator.admission.bloom;

import com.github.benmanes.caffeine.cache.simulator.membership.bloom.DeleteBloomFilter;
import org.testng.annotations.Test;

import static com.google.common.truth.Truth.assertThat;

public class DeleteBloomFilterTest {
    @Test
    public void MeClockDeleteBloomFilterTest() {
        // using fixed seed to make sure the deletion of one element affects the other
        DeleteBloomFilter filter = DeleteBloomFilter.getMeClockDeleteBloomFilter(8, 1.2, 8, 1042313130L);
        long elem = 123L;
        filter.add(elem);
        assertThat(filter.mayContain(elem)).isTrue();
        filter.delete(elem);
        assertThat(filter.mayContain(elem)).isFalse();
        // add the element again with another element
        filter.add(elem);
        long elem2 = 125L;
        filter.add(elem2);
        // assert both elements exist
        assertThat(filter.mayContain(elem)).isTrue();
        assertThat(filter.mayContain(elem2)).isTrue();
        // remove the second element
        filter.delete(elem2);
        assertThat(filter.mayContain(elem2)).isFalse();
        // because of the removal elem1 is also not present anymore
        assertThat(filter.mayContain(elem)).isFalse();
    }

    // Test TBF BFD filter
    @Test
    public void BFDTest() {
        DeleteBloomFilter filter = DeleteBloomFilter.getBFDFilter(8, 1.2, 8, 1042313130L);
        long elem = 123L;
        filter.add(elem);
        assertThat(filter.mayContain(elem)).isTrue();
        long elem2 = 125L;
        filter.add(elem2);
        // delete an element - try multiple times to increase the chance it will be deleted
        for (int i = 0; i < 8; i++) {
            filter.delete(elem);
        }
        assertThat(filter.mayContain(elem)).isFalse();
    }
}
