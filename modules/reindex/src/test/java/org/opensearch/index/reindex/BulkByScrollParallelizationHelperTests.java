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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.reindex;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static org.opensearch.index.reindex.BulkByScrollParallelizationHelper.sliceIntoSubRequests;
import static org.opensearch.search.RandomSearchRequestGenerator.randomSearchRequest;
import static org.opensearch.search.RandomSearchRequestGenerator.randomSearchSourceBuilder;

public class BulkByScrollParallelizationHelperTests extends OpenSearchTestCase {
    public void testSliceIntoSubRequests() throws IOException {
        SearchRequest searchRequest = randomSearchRequest(() -> randomSearchSourceBuilder(
                () -> null,
                () -> null,
                () -> null,
                () -> emptyList(),
                () -> null));
        if (searchRequest.source() != null) {
            // Clear the slice builder if there is one set. We can't call sliceIntoSubRequests if it is.
            searchRequest.source().slice(null);
        }
        int times = between(2, 100);
        String field = randomBoolean() ? IdFieldMapper.NAME : randomAlphaOfLength(5);
        int currentSliceId = 0;
        for (SearchRequest slice : sliceIntoSubRequests(searchRequest, field, times)) {
            assertEquals(field, slice.source().slice().getField());
            assertEquals(currentSliceId, slice.source().slice().getId());
            assertEquals(times, slice.source().slice().getMax());

            // If you clear the slice then the slice should be the same request as the parent request
            slice.source().slice(null);
            if (searchRequest.source() == null) {
                // Except that adding the slice might have added an empty builder
                searchRequest.source(new SearchSourceBuilder());
            }
            assertEquals(searchRequest, slice);
            currentSliceId++;
        }
    }
}
