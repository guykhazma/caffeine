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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import com.github.benmanes.caffeine.cache.simulator.parser.TextTraceReader;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.Characteristic;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;

/**
 * A reader for the IBM ObjectStore trace files provided by
 * <a href="http://iotta.snia.org/traces/36305">SNIA</a>.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
public final class ObjectStoreTraceReader extends TextTraceReader {
  int blockSize = 4 * ((int)(Math.pow(2, 20))); // 4MB

  public ObjectStoreTraceReader(String filePath) {
    super(filePath);
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
          String key = array[2];
          long start = Long.parseLong(array[4]);
          long end = Long.parseLong(array[5]);
          int  weight = Ints.saturatedCast(end - start);
          if (weight < 0) {
              return Stream.empty();
          }
          // generate access requests according to block sizes
          long startBlock = Math.floorDiv(start, blockSize);
          long currBlockID = startBlock;
          long curr = startBlock * blockSize;
          List<AccessEvent> accessEventList = new ArrayList<>();
          while (curr + blockSize <= end) {
            long blockKey = getBlockKey(key, currBlockID);;
            accessEventList.add(AccessEvent.forKeyAndWeight(blockKey, blockSize));
            currBlockID +=1;
            curr += blockSize;
          }
          // generate access for last block
          if (curr < end) {
              long blockKey = getBlockKey(key, currBlockID);
              accessEventList.add(AccessEvent.forKeyAndWeight(blockKey, blockSize));
          }
          return accessEventList.stream();
        });
  }

  // generate key which depends on the block id
  static long getBlockKey(String objectKey, long blockID) {
      return Hashing.sha256()
              .hashString(objectKey + "-" + blockID, StandardCharsets.UTF_8)
              .asLong();
  }
}
