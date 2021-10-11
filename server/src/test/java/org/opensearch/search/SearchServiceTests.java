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

package org.opensearch.search;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.CreatePITAction;
import org.opensearch.action.search.DeletePITAction;
import org.opensearch.action.search.DeletePITRequest;
import org.opensearch.action.search.PITRequest;
import org.opensearch.action.search.PITResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.Strings;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.settings.InternalOrPrivateSettingsPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.rest.RestStatus;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.ShardFetchRequest;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.Collections.singletonList;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason.DELETED;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchHits;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;

public class SearchServiceTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(FailOnRewriteQueryPlugin.class, CustomScriptPlugin.class,
            ReaderWrapperCountPlugin.class, InternalOrPrivateSettingsPlugin.class, MockSearchService.TestPlugin.class);
    }

    public static class ReaderWrapperCountPlugin extends Plugin {
        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.setReaderWrapper(service -> SearchServiceTests::apply);
        }
    }

    @Before
    private void resetCount() {
        numWrapInvocations = new AtomicInteger(0);
    }

    private static AtomicInteger numWrapInvocations = new AtomicInteger(0);
    private static DirectoryReader apply(DirectoryReader directoryReader) throws IOException {
        numWrapInvocations.incrementAndGet();
        return new FilterDirectoryReader(directoryReader,
            new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return reader;
            }
        }) {
            @Override
            protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
                return in;
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return directoryReader.getReaderCacheHelper();
            }
        };
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        static final String DUMMY_SCRIPT = "dummyScript";

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap(DUMMY_SCRIPT, vars -> "dummy");
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onFetchPhase(SearchContext context, long tookInNanos) {
                    if ("throttled_threadpool_index".equals(context.indexShard().shardId().getIndex().getName())) {
                        assertThat(Thread.currentThread().getName(), startsWith("opensearch[node_s_0][search_throttled]"));
                    } else {
                        assertThat(Thread.currentThread().getName(), startsWith("opensearch[node_s_0][search]"));
                    }
                }

                @Override
                public void onQueryPhase(SearchContext context, long tookInNanos) {
                    if ("throttled_threadpool_index".equals(context.indexShard().shardId().getIndex().getName())) {
                        assertThat(Thread.currentThread().getName(), startsWith("opensearch[node_s_0][search_throttled]"));
                    } else {
                        assertThat(Thread.currentThread().getName(), startsWith("opensearch[node_s_0][search]"));
                    }
                }
            });
        }
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put("search.default_search_timeout", "5s").build();
    }

    public void testPIT() throws ExecutionException, InterruptedException {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();

        PITRequest request = new PITRequest(TimeValue.timeValueDays(1));
        request.setIndices(new String[]{"index"});
        ActionFuture<PITResponse> execute = client().execute(CreatePITAction.INSTANCE, request);
        PITResponse pitResponse = execute.get();
        client().prepareIndex("index", "type", "2").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index")
            .setSize(2).setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1))).get();
        assertHitCount(searchResponse, 1);
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(2, service.getActiveContexts());
        client().execute(DeletePITAction.INSTANCE,new DeletePITRequest(pitResponse.getId()));
        assertEquals(0, service.getActiveContexts());
        service.doClose(); // this kills the keep-alive reaper we have to reset the node after this test
    }

    public void testClearOnClose() {
        createIndex("index", Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        client().prepareIndex("index", "type", "1").setSource("field", "value").setRefreshPolicy(IMMEDIATE).get();
        SearchResponse searchResponse = client().prepareSearch("index").setSize(1).setScroll("1m").get();
        assertThat(searchResponse.getScrollId(), is(notNullValue()));
        SearchService service = getInstanceFromNode(SearchService.class);

        assertEquals(1, service.getActiveContexts());
        service.doClose(); // this kills the keep-alive reaper we have to reset the node after this test
        assertEquals(0, service.getActiveContexts());
    }

    public static class FailOnRewriteQueryPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<QuerySpec<?>> getQueries() {
            return singletonList(new QuerySpec<>("fail_on_rewrite_query", FailOnRewriteQueryBuilder::new, parseContext -> {
                throw new UnsupportedOperationException("No query parser for this plugin");
            }));
        }
    }

    public static class FailOnRewriteQueryBuilder extends AbstractQueryBuilder<FailOnRewriteQueryBuilder> {

        public FailOnRewriteQueryBuilder(StreamInput in) throws IOException {
            super(in);
        }

        public FailOnRewriteQueryBuilder() {
        }

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
            if (queryRewriteContext.convertToShardContext() != null) {
                throw new IllegalStateException("Fail on rewrite phase");
            }
            return this;
        }

        @Override
        protected void doWriteTo(StreamOutput out) {
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) {
        }

        @Override
        protected Query doToQuery(QueryShardContext context) {
            return null;
        }

        @Override
        protected boolean doEquals(FailOnRewriteQueryBuilder other) {
            return false;
        }

        @Override
        protected int doHashCode() {
            return 0;
        }

        @Override
        public String getWriteableName() {
            return null;
        }
    }

    private static class ShardScrollRequestTest extends ShardSearchRequest {
        private Scroll scroll;

        ShardScrollRequestTest(ShardId shardId) {
            super(OriginalIndices.NONE, new SearchRequest().allowPartialSearchResults(true),
                shardId, 1, new AliasFilter(null, Strings.EMPTY_ARRAY), 1f, -1, null, null);
            this.scroll = new Scroll(TimeValue.timeValueMinutes(1));
        }

        @Override
        public Scroll scroll() {
            return this.scroll;
        }
    }


    private ReaderContext createReaderContext(IndexService indexService, IndexShard indexShard) {
        return new ReaderContext(new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong()),
            indexService, indexShard, indexShard.acquireSearcherSupplier(), randomNonNegativeLong(), false);
    }
}
