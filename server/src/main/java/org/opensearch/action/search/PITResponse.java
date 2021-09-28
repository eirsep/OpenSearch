/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class PITResponse extends ActionResponse {
    public String getId() {
        return id;
    }

    private final String id;

    PITResponse(String id) {
        this.id = id;
    }

    public PITResponse(StreamInput streamInput) throws IOException {
        id = streamInput.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);

    }
}

