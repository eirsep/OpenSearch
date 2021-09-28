/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportCreatePITAction extends HandledTransportAction<PITRequest, PITResponse> {

    public static final String CREATE_PIT = "create_pit";
    private SearchService searchService;
    private TransportSearchAction transportSearchAction;

    @Inject
    public TransportCreatePITAction(SearchService searchService,
                                    TransportService transportService,
                                    ActionFilters actionFilters,
                                    TransportSearchAction transportSearchAction) {
        super(CreatePITAction.NAME, transportService, actionFilters, in -> new PITRequest(in));
        this.searchService = searchService;
        this.transportSearchAction = transportSearchAction;
    }


    @Override
    protected void doExecute(Task task, PITRequest request, ActionListener<PITResponse> listener) {
        SearchRequest sr = new SearchRequest(request.getIndices());
        sr.preference(request.getPreference());
        sr.routing(request.getRouting());
        sr.indicesOptions(request.getIndicesOptions());
        transportSearchAction.executeRequest(task, sr, CREATE_PIT, true,
            (searchTask, target, connection, sprListener) ->
                searchService.openReaderContext(target.getShardId(), request.getKeepAlive(), new ActionListener<ShardSearchContextId>() {
                    @Override
                    public void onResponse(ShardSearchContextId shardSearchContextId) {
                        PITSinglePhaseSearchResult spr = new PITSinglePhaseSearchResult();
                        spr.setContextId(shardSearchContextId);
                        spr.setSearchShardTarget(target);
                        sprListener.onResponse(spr);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        sprListener.onFailure(e);
                    }
                }), new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    listener.onResponse(new PITResponse(searchResponse.pointInTimeId()));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });

    }

    public static class PITSinglePhaseSearchResult extends SearchPhaseResult {
        public void setContextId(ShardSearchContextId contextId) {
            this.contextId = contextId;
        }
    }
}


