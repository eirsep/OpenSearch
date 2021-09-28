/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

public class PITRequest extends ActionRequest implements IndicesRequest.Replaceable {
    public PITRequest(TimeValue keepAlive, String[] indices) {
        this.keepAlive = keepAlive;
        this.indices = indices;
    }

    public String getRouting() {
        return routing;
    }

    public String getPreference() {
        return preference;
    }

    public String[] getIndices() {
        return indices;
    }

    public IndicesOptions getIndicesOptions() {
        return indicesOptions;
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    private TimeValue keepAlive;

    public PITRequest(StreamInput in) throws IOException {
        super(in);
        indices = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        routing = in.readOptionalString();
        preference = in.readOptionalString();
        keepAlive = in.readTimeValue();
        routing = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(indices);
        indicesOptions.writeIndicesOptions(out);
        out.writeOptionalString(preference);
        out.writeTimeValue(keepAlive);
        out.writeOptionalString(routing);
    }
    @Nullable
    private String routing=null;

    @Nullable
    private String preference=null;

    public void setIndices(String[] indices) {
        this.indices = indices;
    }
    private String[] indices = Strings.EMPTY_ARRAY;

    private IndicesOptions indicesOptions = SearchRequest.DEFAULT_INDICES_OPTIONS;
//    public PITRequest(String[] indices, TimeValue keepAlive) {
//        this.keepAlive = keepAlive;
//        this.indices = indices;

//    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        return null;
    }

    public void setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchTask(id, type, action, () -> "desc", parentTaskId, headers);
    }
}
