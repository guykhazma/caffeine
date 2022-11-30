package org.fastfilter.bloom;

public class BloomUtils {
    // Utility method for getting a bloom filter using num hash functions, num elements, bits per key
    public static Bloom getBloomFilter(int entryCount, double bitsPerKey, int k) {
        return new Bloom(entryCount, bitsPerKey, k);
    }
}
