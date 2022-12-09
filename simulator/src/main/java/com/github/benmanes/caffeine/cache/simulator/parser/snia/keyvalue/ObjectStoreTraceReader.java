/*
 * Copyright 2020 Ben Manes. All Rights Reserved.
 *
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
package com.github.benmanes.caffeine.cache.simulator.parser.snia.keyvalue;

import static com.github.benmanes.caffeine.cache.simulator.policy.Policy.Characteristic.WEIGHTED;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import com.github.benmanes.caffeine.cache.simulator.parser.TextTraceReader;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.Characteristic;

/**
 * A reader for the IBM ObjectStore trace files provided by
 * <a href="http://iotta.snia.org/traces/36305">SNIA</a>.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
public final class ObjectStoreTraceReader extends TextTraceReader {
    int blockSize = 4 * (int) Math.pow(2, 20); // 4MB

    public ObjectStoreTraceReader(String filePath) {
        super(filePath);
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    @Override
    public Set<Characteristic> characteristics() {
        return Set.of(WEIGHTED);
    }

    @Override
    public Stream<AccessEvent> events() {
        return lines()
                .map(line -> line.split(" "))
                .filter(array -> array[1].equals("REST.GET.OBJECT"))
                .flatMap(array -> {
                    long key = new BigInteger(array[2], 16).longValue();
                    long objSize = Long.parseLong(array[3]);
                    long start = Long.parseLong(array[4]);
                    long end = Long.parseLong(array[5]);
                    // if for some reason the request end is larger than the object size fall back to object size as the end
                    // read is inclusive so use -1
                    if (end > objSize - 1) {
                        end = objSize - 1;
                    }
                    // read is inclusive so adding +1
                    // Note that the saturated cast in the original code caused issue since some ranges are larger than 2GB
                    long weight = end - start + 1;

                    // if the object size is less than the block size then count it as weight that is being read
                    // assuming the block size is always smaller than MAX INT
                    if (objSize < blockSize) {
                        return Stream.of(AccessEvent.forKeyAndWeight(key, (int) weight));
                    }

                    // in this case the object spans multiple block so find the relevant blocks
                    // generate access requests according to block sizes
                    long startBlock = Math.floorDiv(start, blockSize);
                    long currBlockID = startBlock;
                    long curr = startBlock * blockSize;
                    // add specific read for the start
                    List<AccessEvent> accessEventList = new ArrayList<>();
                    // add first block
                    long blockKey = getBlockKey(array[2], currBlockID);
                    // we read the entire block even if the start is somewhere in the middle since we cache at the granularity of blocks
                    accessEventList.add(AccessEvent.forKeyAndWeight(blockKey, blockSize));
                    curr += blockSize;
                    currBlockID += 1;
                    while (curr + blockSize <= end) {
                        blockKey = getBlockKey(array[2], currBlockID);;
                        accessEventList.add(AccessEvent.forKeyAndWeight(blockKey, blockSize));
                        currBlockID += 1;
                        curr += blockSize;
                    }
                    // generate access for the block containing the end
                    if (curr < end) {
                        blockKey = getBlockKey(array[2], currBlockID);
                        // if this is the last block of the object then we can read the exact size
                        if (curr + blockSize > objSize) {
                            // read is inclusive
                            accessEventList.add(AccessEvent.forKeyAndWeight(blockKey, (int) (end - curr + 1)));
                        } else {
                            // the end is not the last block of the object so read an entire block
                            accessEventList.add(AccessEvent.forKeyAndWeight(blockKey, blockSize));
                        }
                    }
                    return accessEventList.stream();
                });
    }

    // generate key which depends on the block id
    static long getBlockKey(String objectKey, long blockID) {
        return hash(objectKey + "-" + blockID);
    }

    static long hash(String string) {
        long h = 1125899906842597L; // prime
        int len = string.length();
        for (int i = 0; i < len; i++) {
            h = 31*h + string.charAt(i);
        }
        return h;
    }
}
