package com.github.benmanes.caffeine.cache.simulator.membership.bloom;

import org.fastfilter.Filter;
import org.fastfilter.utils.Hash;

/**
 * Adapted from fast filter bloom filter
 *
 */
public class DeleteBloomFilter implements Filter {
    private final int k;
    private final long seed;
    private final int arraySize;
    private final long[] data;

    @Override
    public long getBitCount() {
        return data.length * 64L;
    }

    public DeleteBloomFilter(int entryCount, double bitsPerKey, int k) {
        entryCount = Math.max(1, entryCount);
        this.k = k;
        this.seed = Hash.randomSeed();
        long bits = (long) (entryCount * bitsPerKey);
        arraySize = (int) ((bits + 63) / 64);
        data = new long[arraySize];
    }

    @Override
    public boolean supportsAdd() {
        return true;
    }

    @Override
    public void add(long key) {
        long hash = Hash.hash64(key, seed);
        long a = (hash >>> 32) | (hash << 32);
        long b = hash;
        for (int i = 0; i < k; i++) {
            data[Hash.reduce((int) (a >>> 32), arraySize)] |= 1L << a;
            a += b;
        }
    }

    @Override
    public boolean mayContain(long key) {
        long hash = Hash.hash64(key, seed);
        long a = (hash >>> 32) | (hash << 32);
        long b = hash;
        double numHits = 0;
        for (int i = 0; i < k; i++) {
            if ((data[Hash.reduce((int) (a >>> 32), arraySize)] & 1L << a) != 0) {
                numHits += 1;
            }
            a += b;
        }
        // an element is assumed to be recorded if at least half of the bits are set to 1
        if (numHits > k / 2.0) {
            return true;
        }
        return false;
    }

    public void delete(long key) {
        long hash = Hash.hash64(key, seed);
        long a = (hash >>> 32) | (hash << 32);
        long b = hash;
        for (int i = 0; i < k; i++) {
            // reset the bit
            data[Hash.reduce((int) (a >>> 32), arraySize)] &= ~(1L << a);
            a += b;
        }
    }

}

