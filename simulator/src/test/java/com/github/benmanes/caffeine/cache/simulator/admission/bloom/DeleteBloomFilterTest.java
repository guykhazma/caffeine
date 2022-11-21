package com.github.benmanes.caffeine.cache.simulator.admission.bloom;

import com.github.benmanes.caffeine.cache.simulator.membership.bloom.DeleteBloomFilter;
import org.testng.annotations.Test;

import static com.google.common.truth.Truth.assertThat;

public class DeleteBloomFilterTest {
    @Test
    public void deleteBloomFilterTest() {
        // 2 billion entries
        DeleteBloomFilter filter = new DeleteBloomFilter(8, 1.2, 8);
        filter.add(123L);
        filter.add(234L);
        assertThat(filter.mayContain(123L)).isTrue();
        filter.delete(123L);
        assertThat(filter.mayContain(123L)).isFalse();
    }
}
