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

package org.opensearch.geometry;

import org.opensearch.geometry.utils.WellKnownText;

/**
 * Circle geometry (not part of WKT standard, but used in opensearch) defined by lat/lon coordinates of the center in degrees
 * and optional altitude in meters.
 */
public class Circle implements Geometry {
    public static final Circle EMPTY = new Circle();
    private final double y;
    private final double x;
    private final double z;
    private final double radiusMeters;

    private Circle() {
        y = 0;
        x = 0;
        z = Double.NaN;
        radiusMeters = -1;
    }

    public Circle(final double x, final double y, final double radiusMeters) {
        this(x, y, Double.NaN, radiusMeters);
    }

    public Circle(final double x, final double y, final double z, final double radiusMeters) {
        this.y = y;
        this.x = x;
        this.radiusMeters = radiusMeters;
        this.z = z;
        if (radiusMeters < 0 ) {
            throw new IllegalArgumentException("Circle radius [" + radiusMeters + "] cannot be negative");
        }
    }

    @Override
    public ShapeType type() {
        return ShapeType.CIRCLE;
    }

    public double getY() {
        return y;
    }

    public double getX() {
        return x;
    }

    public double getRadiusMeters() {
        return radiusMeters;
    }

    public double getZ() {
        return z;
    }

    public double getLat() {
        return y;
    }

    public double getLon() {
        return x;
    }

    public double getAlt() {
        return z;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Circle circle = (Circle) o;
        if (Double.compare(circle.y, y) != 0) return false;
        if (Double.compare(circle.x, x) != 0) return false;
        if (Double.compare(circle.radiusMeters, radiusMeters) != 0) return false;
        return (Double.compare(circle.z, z) == 0);
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        temp = Double.doubleToLongBits(y);
        result = (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(x);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(radiusMeters);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(z);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public <T, E extends Exception> T visit(GeometryVisitor<T, E> visitor) throws E {
        return visitor.visit(this);
    }

    @Override
    public boolean isEmpty() {
        return radiusMeters < 0;
    }

    @Override
    public String toString() {
        return WellKnownText.INSTANCE.toWKT(this);
    }

    @Override
    public boolean hasZ() {
        return Double.isNaN(z) == false;
    }
}
