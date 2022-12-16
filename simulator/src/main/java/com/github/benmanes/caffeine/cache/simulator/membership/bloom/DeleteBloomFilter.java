package com.github.benmanes.caffeine.cache.simulator.membership.bloom;

import org.fastfilter.Filter;
import org.fastfilter.utils.Hash;

import java.util.Random;


/**
 * Bloom filter implementation that supports delete and variable sized check
 * Possible knobs -
 * - Number of bits to set when inserting to the bloom filter (-1 indicates select randomly on each insertion)
 * - Number of bits to delete when deleting the element (-1 indicates select randomly on each deletion)
 * - Number of bits to use when determining if an element is in the bloom filter
 */
public class DeleteBloomFilter implements Filter {
    public final int numHashFunctions;
    private final long seed;
    private final int arraySize;
    private long[] data;
    // the threshold to determine if an element is in the bloom filter
    private final int existThreshold;
    // The number of bits to set on insert
    private int numBitsToSetOnInsert;
    // The number of reset when delete
    private int numBitsToResetOnDelete;
    // random for selecting which bits to set/reset
    private Random r = new Random();

    @Override
    public long getBitCount() {
        return data.length * 64L;
    }

    public static DeleteBloomFilter getBloomFilter(long numElements, double bitsPerKey, int numHashFunctions) {
        return new DeleteBloomFilter(numElements, bitsPerKey, numHashFunctions,
                numHashFunctions, numHashFunctions, numHashFunctions, Hash.randomSeed());
    }

    public static DeleteBloomFilter getBFDFilter(long numElements, double bitsPerKey, int numHashFunctions) {
        return getBFDFilter(numElements, bitsPerKey, numHashFunctions, Hash.randomSeed());
    }
    // Bloom filter variant presented in the TBF paper
    // This filter deletion by resetting some of the bits randomly
    // Check is done with all bits
    // Insertion is done with all bits
    public static DeleteBloomFilter getBFDFilter(long numElements, double bitsPerKey, int numHashFunctions, long seed) {
        return new DeleteBloomFilter(numElements, bitsPerKey, numHashFunctions,
                numHashFunctions, numHashFunctions, -1,
                seed);
    }

    public static DeleteBloomFilter getMeClockDeleteBloomFilter(long numElements, double bitsPerKey, int numHashFunctions) {
        return getMeClockDeleteBloomFilter(numElements, bitsPerKey, numHashFunctions, Hash.randomSeed());
    }

    public static DeleteBloomFilter getMeClockDeleteBloomFilter(long numElements, double bitsPerKey,
                                                                int numHashFunctions, long seed) {
        return new DeleteBloomFilter(numElements, bitsPerKey, numHashFunctions,
                numHashFunctions / 2 + 1, numHashFunctions, numHashFunctions,
                seed);
    }

    public DeleteBloomFilter(long numElements, double bitsPerKey, int numHashFunctions,
                             int existThreshold, int numBitsToSetOnInsert, int numBitsToResetOnDelete) {
        this(numElements, bitsPerKey, numHashFunctions,
                existThreshold, numBitsToSetOnInsert, numBitsToResetOnDelete, Hash.randomSeed());
    }

    // constructor fixed seed for tests
    public DeleteBloomFilter(long numElements, double bitsPerKey, int numHashFunctions,
                             int existThreshold, int numBitsToSetOnInsert, int numBitsToResetOnDelete,
                             long seed) {
        numElements = Math.max(1, numElements);
        this.numHashFunctions = numHashFunctions;
        this.existThreshold = existThreshold;
        this.numBitsToSetOnInsert = numBitsToSetOnInsert;
        this.numBitsToResetOnDelete = numBitsToResetOnDelete;
        this.seed = seed;
        long bits = (long) (numElements * bitsPerKey);
        arraySize = (int) ((bits + 63) / 64);
        data = new long[arraySize];
    }

    @Override
    public boolean supportsAdd() {
        return true;
    }

    // reset all of the bits in the bloom filter
    public void reset() {
        data = new long[arraySize];
    }

    public void add(long key, int numBitsToSet) {
        int currNumBitsSet = 0;
        long hash = Hash.hash64(key, seed);
        long a = (hash >>> 32) | (hash << 32);
        long b = hash;
        for (int i = 0; i < numHashFunctions && currNumBitsSet < numBitsToSet; i++) {
            if (numBitsToSet == numHashFunctions) {
                data[Hash.reduce((int) (a >>> 32), arraySize)] |= 1L << a;
                currNumBitsSet += 1;
            }
            // make sure to set exactly the num bits asked
            // so if a bit is already set to 1 don't count it
            else if ((data[Hash.reduce((int) (a >>> 32), arraySize)] & 1L << a) == 0) {
                data[Hash.reduce((int) (a >>> 32), arraySize)] |= 1L << a;
                currNumBitsSet += 1;
            }
            a += b;
        }
    }

    @Override
    public void add(long key) {
        int numBitsToSet = numBitsToSetOnInsert;
        // if we need to draw a number of bits to delete
        if (numBitsToSet == -1) {
            numBitsToSet = r.nextInt(numHashFunctions) + 1;
        }
        add(key, numBitsToSet);
    }

    public int numBitsSet(long key) {
        long hash = Hash.hash64(key, seed);
        long a = (hash >>> 32) | (hash << 32);
        long b = hash;
        int numHits = 0;
        for (int i = 0; i < numHashFunctions; i++) {
            if ((data[Hash.reduce((int) (a >>> 32), arraySize)] & 1L << a) != 0) {
                numHits += 1;
            }
            a += b;
        }
        return numHits;
    }

    @Override
    public boolean mayContain(long key) {
        // an element is assumed to be recorded if at least half of the bits are set to 1
        if (numBitsSet(key) >= this.existThreshold) {
            return true;
        }
        return false;
    }

    public void delete(long key, int numBitsToDelete) {
        int currNumBitsDeleted = 0;
        long hash = Hash.hash64(key, seed);
        long a = (hash >>> 32) | (hash << 32);
        long b = hash;
        for (int i = 0; i < numHashFunctions && currNumBitsDeleted < numBitsToDelete; i++) {
            // delete the bit either if we need to delete all of the bits or if the hash function
            // was randomly selected and still less than the amount of bits to delete
            if (numBitsToDelete == numHashFunctions) {
                data[Hash.reduce((int) (a >>> 32), arraySize)] &= ~(1L << a);
                currNumBitsDeleted += 1;
            }
            else if ((data[Hash.reduce((int) (a >>> 32), arraySize)] & 1L << a) != 0) {
                data[Hash.reduce((int) (a >>> 32), arraySize)] &= ~(1L << a);
                currNumBitsDeleted += 1;
            }
            a += b;
        }
    }

    public void delete(long key) {
        int numBitsToDelete = numBitsToResetOnDelete;
        // if we need to draw a number of bits to delete
        if (numBitsToDelete == -1) {
            numBitsToDelete = r.nextInt(numHashFunctions) + 1;
        }
        delete(key, numBitsToDelete);
    }
}

