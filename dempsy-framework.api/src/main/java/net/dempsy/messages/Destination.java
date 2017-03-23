/*
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.dempsy.messages;

import net.dempsy.config.ClusterId;

/**
 * This class represents and opaque handle to message transport implementation specific means of connecting to a destination.
 */
public abstract class Destination {
    public final String nodeId;
    public final ClusterId clusterId;

    public Destination(final String nodeId, final ClusterId clusterId) {
        this.nodeId = nodeId;
        this.clusterId = clusterId;
    }
}
