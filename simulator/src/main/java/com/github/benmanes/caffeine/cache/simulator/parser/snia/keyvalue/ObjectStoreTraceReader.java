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
import com.google.common.primitives.Ints;

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
          int objSize = Ints.saturatedCast(Long.parseLong(array[3]));
          long start = Long.parseLong(array[4]);
          long end = Long.parseLong(array[5]);
          int  weight = Ints.saturatedCast(end - start);
          // filter invalid events
          if (weight < 0) {
              return Stream.empty();
          }

          // if the object size is less than the block size then count it as the actual length
          if (objSize < blockSize) {
              return Stream.of(AccessEvent.forKeyAndWeight(key, objSize));
          }
          // generate access requests according to block sizes
          long startBlock = Math.floorDiv(start, blockSize);
          long currBlockID = startBlock;
          long curr = startBlock * blockSize;
          // add specific read for the start
          List<AccessEvent> accessEventList = new ArrayList<>();
          // add first block
          long blockKey = getBlockKey(array[2], currBlockID);
          accessEventList.add(AccessEvent.forKeyAndWeight(blockKey, (int) (curr + blockSize - start)));
          curr += blockSize;
          currBlockID += 1;
          while (curr + blockSize <= end) {
            blockKey = getBlockKey(array[2], currBlockID);;
            accessEventList.add(AccessEvent.forKeyAndWeight(blockKey, blockSize));
            currBlockID += 1;
            curr += blockSize;
          }
          // generate access for last block according to the size of the block
          if (curr < end) {
              blockKey = getBlockKey(array[2], currBlockID);
              accessEventList.add(AccessEvent.forKeyAndWeight(blockKey, (int) (end - curr)));
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
