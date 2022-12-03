/*
 * Copyright 2015 Ben Manes. All Rights Reserved.
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
package com.github.benmanes.caffeine.cache.simulator.policy.linked;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.Admission;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.membership.bloom.DeleteBloomFilter;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy.PolicySpec;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.MoreObjects;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Set;
import java.util.function.Predicate;

import static com.github.benmanes.caffeine.cache.simulator.policy.Policy.Characteristic.WEIGHTED;
import static java.util.stream.Collectors.toUnmodifiableSet;

/**
 * Me-Clock policy as described in the paper
 */
@PolicySpec(characteristics = WEIGHTED)
public final class MeClockPolicyAdaptive implements Policy {
  final Long2ObjectMap<Node> data;
  final PolicyStats policyStats;
  final EvictionPolicy policy;
  final Admittor admittor;
  final long maximumSize;
  final boolean weighted;
  final Node sentinel;
  final DeleteBloomFilter doorkeeper;
  final DeleteBloomFilter ghost;
  // the threshold to determine if an element is in the bloom filter
  int existThreshold;
  // The number of bits to set on insert
  int numBitsToSetOnInsert;
  // The number of reset when delete
  int numBitsToResetOnDelete;

  long currentSize;

  public MeClockPolicyAdaptive(Config config, Set<Characteristic> characteristics,
                               Admission admission, EvictionPolicy policy) {
    this.policyStats = new PolicyStats(admission.format(policy.label()));
    this.admittor = admission.from(config, policyStats);
    this.weighted = characteristics.contains(WEIGHTED);

    MeClockAdaptiveSettings settings = new MeClockAdaptiveSettings(config);
    this.data = new Long2ObjectOpenHashMap<>();
    this.maximumSize = settings.maximumSize();
    this.sentinel = new Node();
    this.policy = policy;
    // set initial parameters
    this.numBitsToSetOnInsert = settings.numHashFunctions() / 4;
    this.numBitsToResetOnDelete = settings.numHashFunctions() / 4;
    this.existThreshold = settings.numHashFunctions() / 2;
    // get the bloom filter parameters
    this.doorkeeper = new DeleteBloomFilter(settings.numElements(), settings.bitsPerKey(), settings.numHashFunctions(),
            this.existThreshold, this.numBitsToSetOnInsert, this.numBitsToResetOnDelete);
    this.ghost = new DeleteBloomFilter(settings.numElements(), settings.bitsPerKey(), settings.numHashFunctions(),
            settings.numHashFunctions(), settings.numHashFunctions(), settings.numHashFunctions());
  }

  /**
   * Returns all variations of this policy based on the configuration parameters.
   */
  public static Set<Policy> policies(Config config,
      Set<Characteristic> characteristics, EvictionPolicy policy) {
    BasicSettings settings = new BasicSettings(config);
    return settings.admission().stream().map(admission -> new MeClockPolicyAdaptive(config, characteristics, admission, policy))
        .collect(toUnmodifiableSet());
  }

  @Override
  public PolicyStats stats() {
    return policyStats;
  }

  @Override
  public void record(AccessEvent event) {
    final int weight = weighted ? event.weight() : 1;
    final long key = event.key();
    Node old = data.get(key);
    admittor.record(key);
    if (old == null) { // add
      policyStats.recordWeightedMiss(weight);
      if (weight > maximumSize) {
        policyStats.recordOperation();
        return;
      }
      Node node = new Node(key, weight, sentinel);
      // add to the bloom filter to keep track of recency
      doorkeeper.add(key);
      data.put(key, node);
      currentSize += node.weight;
      node.appendToTail();
      evict(node);
    } else { // hit
      policyStats.recordWeightedHit(weight);
      currentSize += (weight - old.weight);
      old.weight = weight;
      // this addition will increase the "frequency" by setting more bits
      doorkeeper.add(key);
      policy.onAccess(old, policyStats);
      // should never really evict anything
      evict(old);
    }
  }

  /** Evicts while the map exceeds the maximum capacity. */
  private void evict(Node candidate) {
    if (currentSize > maximumSize) {
      // extract the condition we search for by default the exist condition
      Predicate<Integer> condition = i -> (i >= existThreshold);
      // check if the item was seen in history
      int numBitsSet = ghost.numBitsSet(candidate.key);
      if (numBitsSet > 0) {
        // this entry was referenced only once in the history (equivalent to hit on B1)
        if (numBitsSet == numBitsToSetOnInsert) {
          // we should evict frequent item so searching for frequent items (from T2)
          condition =  i -> (i >= doorkeeper.numHashFunctions / 2);
          // next time we insert a new item we want to evict
          existThreshold = Math.min(existThreshold, existThreshold - 1);
        } else if (numBitsSet > numBitsToSetOnInsert) {
          // this entry was referenced more than once - similar to hit in B2
          // we should evict a recent (from T1)
          condition =  i -> (i < doorkeeper.numHashFunctions / 2 && i >= doorkeeper.numHashFunctions / 4);
          existThreshold = Math.min(existThreshold + 1, doorkeeper.numHashFunctions);
        }
        // add back to bloom filter with the historical value
        doorkeeper.add(candidate.key, numBitsSet);
        // remove from ghost cache as the element is promoted to cache
        ghost.delete(candidate.key);
      }
      while (currentSize > maximumSize) {
        if (candidate.weight > maximumSize) {
          doorkeeper.delete(candidate.key);
          evictEntry(candidate);
          continue;
        }

        // This key was not seen before replace according to the current threshold
        Node victim = policy.findVictim(sentinel, policyStats);
        int victimNumBits = doorkeeper.numBitsSet(victim.key);
        if (condition.test(victimNumBits)) { // recycle if needed
          doorkeeper.delete(victim.key);
          victim.moveToTail();
        } else { // replace
          evictEntry(victim);
          // add to ghost bloom filter with the same number of bits
          ghost.add(victim.key, numBitsSet);
        }
      }
    } else {
      policyStats.recordOperation();
    }
  }

  private void evictEntry(Node node) {
    policyStats.recordEviction();
    currentSize -= node.weight;
    data.remove(node.key);
    node.remove();
  }

  /** The replacement policy. */
  public enum EvictionPolicy {
    ME_CLOCK {
      @Override
      void onAccess(Node node, PolicyStats policyStats) {
        policyStats.recordOperation();
        // do nothing
      }

      @Override
      Node findVictim(Node sentinel, PolicyStats policyStats) {
        policyStats.recordOperation();
        return sentinel.next;
      }
    };

    public String label() {
      return "linked.meclockadaptive";
    }

    /**
     * Performs any operations required by the policy after a node was successfully
     * retrieved.
     */
    abstract void onAccess(Node node, PolicyStats policyStats);

    /** Returns the victim entry to evict. */
    abstract Node findVictim(Node sentinel, PolicyStats policyStats);
  }

  /** A node on the double-linked list. */
  static final class Node {
    final Node sentinel;

    boolean marked;
    Node prev;
    Node next;
    long key;
    int weight;

    /** Creates a new sentinel node. */
    public Node() {
      this.key = Long.MIN_VALUE;
      this.sentinel = this;
      this.prev = this;
      this.next = this;
    }

    /** Creates a new, unlinked node. */
    public Node(long key, int weight, Node sentinel) {
      this.sentinel = sentinel;
      this.key = key;
      this.weight = weight;
    }

    /** Appends the node to the tail of the list. */
    public void appendToTail() {
      Node tail = sentinel.prev;
      sentinel.prev = this;
      tail.next = this;
      next = sentinel;
      prev = tail;
    }

    /** Removes the node from the list. */
    public void remove() {
      prev.next = next;
      next.prev = prev;
      prev = next = null;
      key = Long.MIN_VALUE;
    }

    /** Moves the node to the tail. */
    public void moveToTail() {
      // unlink
      prev.next = next;
      next.prev = prev;

      // link
      next = sentinel;
      prev = sentinel.prev;
      sentinel.prev = this;
      prev.next = this;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("key", key)
          .add("weight", weight)
          .add("marked", marked)
          .toString();
    }
  }

  static final class MeClockAdaptiveSettings extends BasicSettings {
    public MeClockAdaptiveSettings(Config config) {
      super(config);
    }
    public int numHashFunctions() {
      return config().getInt("me-clock-adaptive.num-hash-functions");
    }
    public double bitsPerKey() {
      return config().getDouble("me-clock-adaptive.bits-per-key");
    }
    public long numElements() {
      return config().getLong("me-clock-adaptive.num-elements");
    }
  }
}
