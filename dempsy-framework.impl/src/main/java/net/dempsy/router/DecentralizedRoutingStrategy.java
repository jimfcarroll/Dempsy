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

import org.springframework.beans.factory.annotation.Autowired;

import net.dempsy.cluster.ClusterInfoSession;

/**
 * This Routing Strategy uses the MpCluster to negotiate with other instances in the cluster.
 */
public class DecentralizedRoutingStrategy implements RoutingStrategy {

    private final int defaultTotalSlots;
    private final int defaultNumNodes;

    ClusterInfoSession colab;

    public DecentralizedRoutingStrategy(final int defaultTotalSlots, final int defaultNumNodes) {
        this.defaultTotalSlots = defaultTotalSlots;
        this.defaultNumNodes = defaultNumNodes;
    }

    @Override
    public MpContainerAddress selectDestinationForMessage(final Object messageKey, final Object message) {
        return null;
    }

    @Autowired
    public void setCollaborator(final ClusterInfoSession colab) {
        this.colab = colab;

    }

    @PostConstruct
    public void initialize() {

    }
}
