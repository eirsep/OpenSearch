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

package org.opensearch.search.aggregations.bucket.nested;

import org.opensearch.common.ParsingException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.AggregatorFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class NestedAggregationBuilder extends AbstractAggregationBuilder<NestedAggregationBuilder> {
    public static final String NAME = "nested";

    private final String path;

    /**
     * @param name
     *            the name of this aggregation
     * @param path
     *            the path to use for this nested aggregation. The path must
     *            match the path to a nested object in the mappings.
     */
    public NestedAggregationBuilder(String name, String path) {
        super(name);
        if (path == null) {
            throw new IllegalArgumentException("[path] must not be null: [" + name + "]");
        }
        this.path = path;
    }

    protected NestedAggregationBuilder(NestedAggregationBuilder clone, Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.path = clone.path;
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new NestedAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public NestedAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        path = in.readString();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(path);
    }

    /**
     * Get the path to use for this nested aggregation.
     */
    public String path() {
        return path;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    @Override
    protected AggregatorFactory doBuild(QueryShardContext queryShardContext,
                                            AggregatorFactory parent,
                                            Builder subFactoriesBuilder) throws IOException {
        ObjectMapper childObjectMapper = queryShardContext.getObjectMapper(path);
        if (childObjectMapper == null) {
            // in case the path has been unmapped:
            return new NestedAggregatorFactory(name, null, null, queryShardContext,
                parent, subFactoriesBuilder, metadata);
        }

        if (childObjectMapper.nested().isNested() == false) {
            throw new AggregationExecutionException("[nested] nested path [" + path + "] is not nested");
        }
        try {
            ObjectMapper parentObjectMapper = queryShardContext.nestedScope().nextLevel(childObjectMapper);
            return new NestedAggregatorFactory(name, parentObjectMapper, childObjectMapper, queryShardContext,
                parent, subFactoriesBuilder, metadata);
        } finally {
            queryShardContext.nestedScope().previousLevel();
        }
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NestedAggregator.PATH_FIELD.getPreferredName(), path);
        builder.endObject();
        return builder;
    }

    public static NestedAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        String path = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (NestedAggregator.PATH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    path = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + aggregationName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + aggregationName + "].");
            }
        }

        if (path == null) {
            // "field" doesn't exist, so we fall back to the context of the ancestors
            throw new ParsingException(parser.getTokenLocation(), "Missing [path] field for nested aggregation [" + aggregationName + "]");
        }

        return new NestedAggregationBuilder(aggregationName, path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        NestedAggregationBuilder other = (NestedAggregationBuilder) obj;
        return Objects.equals(path, other.path);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
