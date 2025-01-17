/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.routing;

import org.apache.lucene.util.CollectionUtil;
import org.opensearch.common.util.Countable;

import java.util.Iterator;
import java.util.List;

/**
 * This class implements a compilation of {@link ShardIterator}s. Each {@link ShardIterator}
 * iterated by this {@link Iterable} represents a group of shards.
 * ShardsIterators are always returned in ascending order independently of their order at construction
 * time. The incoming iterators are sorted to ensure consistent iteration behavior across Nodes / JVMs.
*/
public final class GroupShardsIterator<ShardIt extends Comparable<ShardIt> & Countable> implements Iterable<ShardIt> {

    private final List<ShardIt> iterators;

    /**
     * Constructs a new sorted GroupShardsIterator from the given list. Items are sorted based on their natural ordering.
     * @see PlainShardIterator#compareTo(ShardIterator)
     */
    public static <ShardIt extends Comparable<ShardIt> & Countable> GroupShardsIterator<ShardIt> sortAndCreate(List<ShardIt> iterators) {
        CollectionUtil.timSort(iterators);
        return new GroupShardsIterator<>(iterators);
    }

    /**
     * Constructs a new GroupShardsIterator from the given list.
     */
    public GroupShardsIterator(List<ShardIt> iterators) {
        this.iterators = iterators;
    }

    /**
     * Returns the total number of shards within all groups
     * @return total number of shards
     */
    public int totalSize() {
        return iterators.stream().mapToInt(Countable::size).sum();
    }

    /**
     * Returns the total number of shards plus the number of empty groups
     * @return number of shards and empty groups
     */
    public int totalSizeWith1ForEmpty() {
        int size = 0;
        for (ShardIt shard : iterators) {
            size += Math.max(1, shard.size());
        }
        return size;
    }

    /**
     * Return the number of groups
     * @return number of groups
     */
    public int size() {
        return iterators.size();
    }

    @Override
    public Iterator<ShardIt> iterator() {
        return iterators.iterator();
    }

    public ShardIt get(int index) {
        return  iterators.get(index);
    }
}
