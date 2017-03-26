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

package net.dempsy.router;

import javax.annotation.PostConstruct;

import net.dempsy.Infrastructure;
import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.messages.KeyedMessageWithType;

/**
 * This Routing Strategy uses the MpCluster to negotiate with other instances in the cluster.
 */
public class DecentralizedRoutingStrategy implements RoutingStrategy {

    private final int defaultTotalSlots;
    private final int defaultNumNodes;

    private ClusterInfoSession colab;
    private String rootDir;

    public DecentralizedRoutingStrategy(final int defaultTotalSlots, final int defaultNumNodes) {
        this.defaultTotalSlots = defaultTotalSlots;
        this.defaultNumNodes = defaultNumNodes;
    }

    @Override
    public ContainerAddress selectDestinationForMessage(final KeyedMessageWithType message) {
        return null;
    }

    @Override
    public void start(final Infrastructure infra) {
        this.colab = infra.getCollaborator();
        this.rootDir = infra.getRootPaths().clustersDir;
    }

    @PostConstruct
    public void initialize() {

    }
}
