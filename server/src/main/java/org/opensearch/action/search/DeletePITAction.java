/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.master.AcknowledgedResponse;

public class DeletePITAction extends ActionType<DeletePITResponse> { //todo change ackResponse to something custom

    public static final DeletePITAction INSTANCE = new DeletePITAction();
    public static final String NAME = "indices:data/delete/pit";

    private DeletePITAction() {
        super(NAME, DeletePITResponse::new);
    }
}
