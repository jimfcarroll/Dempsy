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

package net.dempsy.config;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import net.dempsy.messages.Adaptor;
import net.dempsy.messages.KeySource;
import net.dempsy.messages.MessageProcessorLifecycle;

/**
 * <p>
 * A {@link Cluster} is part of an {@link ApplicationDefinition}. For a full description of the {@link Cluster} please see the {@link ApplicationDefinition} documentation.
 * </p>
 * 
 * <p>
 * Note: for ease of use when configuring by hand (not using a dependency injection framework like Spring or Guice) all "setters" (all 'mutators' in general) return the {@link Cluster} itself for the
 * purpose of chaining.
 * </p>
 */
public class Cluster {
    private ClusterId clusterId;
    private MessageProcessorLifecycle<?> mp = null;
    private Adaptor adaptor = null;
    private String routingStrategyId;
    private ClusterId[] destinations = {};

    private KeySource<?> keySource = null;
    private EvictionFrequency evictionFrequency = new EvictionFrequency(600L, TimeUnit.SECONDS);

    public static class EvictionFrequency {
        public final long evictionFrequency;
        public final TimeUnit evictionTimeUnit;

        public EvictionFrequency(final long evictionFrequency, final TimeUnit evictionTimeUnit) {
            this.evictionFrequency = evictionFrequency;
            this.evictionTimeUnit = evictionTimeUnit;
        }
    }

    /**
     * Create a ClusterDefinition from a cluster name. A {@link Cluster} is to be embedded in an {@link ApplicationDefinition} so it only needs to cluster name and not the entire {@link ClusterId}.
     */
    Cluster(final String applicationName, final String clusterName) {
        this.clusterId = new ClusterId(applicationName, clusterName);
    }

    public Cluster(final String clusterName) {
        this.clusterId = new ClusterId(null, clusterName);
    }

    // ============================================================================
    // Builder functionality.
    // ============================================================================
    /**
     * Set the list of explicit destination that outgoing messages should be limited to.
     */
    public Cluster destination(final String... destinations) {
        final String applicationName = clusterId.applicationName;
        return destination(Arrays.stream(destinations).map(d -> new ClusterId(applicationName, d)).toArray(ClusterId[]::new));
    }

    /**
     * Set the list of explicit destination that outgoing messages should be limited to.
     */
    public Cluster destination(final ClusterId... destinations) {
        this.destinations = destinations;
        return this;
    }

    public Cluster mp(final MessageProcessorLifecycle<?> messageProcessor) throws IllegalStateException {
        if (this.mp != null)
            throw new IllegalStateException("MessageProcessorLifecycle already set on cluster " + clusterId);
        if (this.adaptor != null)
            throw new IllegalStateException("Adaptor already set on cluster " + clusterId + ". Cannot also set a MessageProcessorLifecycle");

        this.mp = messageProcessor;
        return this;
    }

    public Cluster adaptor(final Adaptor adaptor) throws IllegalStateException {
        if (this.adaptor != null)
            throw new IllegalStateException("Adaptor already set on cluster " + clusterId);
        if (this.mp != null)
            throw new IllegalStateException("MessageProcessorLifecycle already set on cluster " + clusterId + ". Cannot also set an Adaptor");
        this.adaptor = adaptor;
        return this;
    }

    public Cluster routing(final String routingStrategyId) {
        this.routingStrategyId = routingStrategyId;
        return this;
    }

    public Cluster keySource(final KeySource<?> keySource) {
        this.keySource = keySource;
        return this;
    }

    public Cluster evictionFrequency(final long evictionFrequency, final TimeUnit timeUnit) {
        this.evictionFrequency = new EvictionFrequency(evictionFrequency, timeUnit);
        return this;
    }
    // ============================================================================

    // ============================================================================
    // Java Bean functionality
    // ============================================================================
    public KeySource<?> getKeySource() {
        return keySource;
    }

    public Cluster setKeySource(final KeySource<?> keySource) {
        this.keySource = keySource;
        return this;
    }

    public EvictionFrequency getEvictionFrequency() {
        return evictionFrequency;
    }

    public Cluster setEvictionFrequency(final EvictionFrequency evictionFrequency) {
        this.evictionFrequency = evictionFrequency;
        return this;
    }

    /**
     * Get the full clusterId of this cluster.
     */
    public ClusterId getClusterId() {
        return clusterId;
    }

    /**
     * If this {@link Cluster} identifies specific destination for outgoing messages, this will return the list of ids of those destination clusters.
     */
    public ClusterId[] getDestinations() {
        return destinations;
    }

    /**
     * Set the list of explicit destination that outgoing messages should be limited to.
     */
    public Cluster setDestinations(final ClusterId... destinations) {
        return destination(destinations);
    }

    public String getRoutingStrategyId() {
        return routingStrategyId;
    }

    public Cluster setRoutingStrategyId(final String routingStrategyId) {
        return routing(routingStrategyId);
    }

    public Cluster setMessageProcessor(final MessageProcessorLifecycle<?> mp) {
        return mp(mp);
    }

    public MessageProcessorLifecycle<?> getMessageProcessor() {
        return mp;
    }

    public Cluster setAdaptor(final Adaptor adaptor) {
        return adaptor(adaptor);
    }

    public Adaptor getAdaptor() {
        return adaptor;
    }
    // ============================================================================

    /**
     * Returns true if there are any explicitly defined destinations.
     * 
     * @see #setDestinations
     */
    public boolean hasExplicitDestinations() {
        return this.destinations != null && this.destinations.length > 0;
    }

    public boolean isAdaptor() {
        return (adaptor != null);
    }

    // This is called from Node
    void setAppName(final String appName) {
        if (clusterId.applicationName != null && !clusterId.applicationName.equals(appName))
            throw new IllegalStateException("Restting the application name on a cluster is not allowed.");
        clusterId = new ClusterId(appName, clusterId.clusterName);
    }

    public void validate() throws IllegalStateException {
        if (mp == null && adaptor == null)
            throw new IllegalStateException("A dempsy cluster must contain either an 'adaptor' or a message processor prototype. " +
                    clusterId + " doesn't appear to be configure with either.");
        if (mp != null && adaptor != null)
            throw new IllegalStateException("A dempsy cluster must contain either an 'adaptor' or a message processor prototype but not both. " +
                    clusterId + " appears to be configured with both.");

        if (mp != null)
            mp.validate();

        if (adaptor != null && keySource != null)
            throw new IllegalStateException("A dempsy cluster can not pre-instantation an adaptor.");

        if (routingStrategyId == null)
            throw new IllegalStateException("No routing strategy set for " + clusterId + ". This should be set on the "
                    + Cluster.class.getSimpleName() + " or on the " + Node.class.getSimpleName());
    }

}
