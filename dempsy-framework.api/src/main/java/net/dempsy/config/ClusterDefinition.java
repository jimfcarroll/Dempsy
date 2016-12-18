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

import net.dempsy.lifecycle.annotations.MessageProcessor;
import net.dempsy.lifecycle.annotations.Mp;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.KeySource;
import net.dempsy.util.SafeString;

/**
 * <p>
 * A {@link ClusterDefinition} is part of an {@link ApplicationDefinition}. For a full description of the {@link ClusterDefinition} please see the {@link ApplicationDefinition} documentation.
 * </p>
 * 
 * <p>
 * Note: for ease of use when configuring by hand (not using a dependency injection framework like Spring or Guice) all "setters" (all 'mutators' in general) return the {@link ClusterDefinition} itself for the
 * purpose of chaining.
 * </p>
 */
public class ClusterDefinition {
    private ClusterId clusterId;
    private final String clusterName;

    private MessageProcessor messageProcessorPrototype;
    private Adaptor adaptor;
    private ClusterId[] destinations = {};
    private Object statsCollectorFactory = null;
    private Object dempsyExecutor = null;
    private Object outputExecuter = null;
    private boolean adaptorIsDaemon = false;
    private KeySource<?> keySource = null;
    private long evictionFrequency = 600;
    private TimeUnit evictionTimeUnit = TimeUnit.SECONDS;

    private ApplicationDefinition parent;

    /**
     * Create a ClusterDefinition from a cluster name. A {@link ClusterDefinition} is to be embedded in an {@link ApplicationDefinition} so it only needs to cluster name and not the entire {@link ClusterId}.
     */
    public ClusterDefinition(final String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * <p>
     * Create a ClusterDefinition from a cluster name. A {@link ClusterDefinition} is to be embedded in an {@link ApplicationDefinition} so it only needs to cluster name and not the entire {@link ClusterId}.
     * </p>
     * 
     * <p>
     * This Cluster will represent an {@link Adaptor} cluster. Use this constructor from a dependency injection framework supporting constructor injection.
     * </p>
     * 
     * @see #setAdaptor
     */
    public ClusterDefinition(final String clusterName, final Adaptor adaptor) {
        this.clusterName = clusterName;
        setAdaptor(adaptor);
    }

    /**
     * <p>
     * Create a ClusterDefinition from a cluster name. A {@link ClusterDefinition} is to be embedded in an {@link ApplicationDefinition} so it only needs to cluster name and not the entire {@link ClusterId}.
     * </p>
     * 
     * <p>
     * This Cluster will represent a {@link Mp} cluster. Use this constructor from a dependency injection framework supporting constructor injection.
     * </p>
     * 
     * @see #setMessageProcessorPrototype
     */
    public ClusterDefinition(final String clusterName, final MessageProcessor prototype) {
        this.clusterName = clusterName;
        setMessageProcessor(prototype);
    }

    /**
     * Get the full clusterId of this cluster.
     */
    public ClusterId getClusterId() {
        return clusterId;
    }

    /**
     * If this {@link ClusterDefinition} identifies specific destination for outgoing messages, this will return the list of ids of those destination clusters.
     */
    public ClusterId[] getDestinations() {
        return destinations;
    }

    /**
     * Set the list of explicit destination that outgoing messages should be limited to.
     */
    public ClusterDefinition setDestinations(final ClusterId... destinations) {
        this.destinations = destinations;
        return this;
    }

    /**
     * Returns true if there are any explicitly defined destinations.
     * 
     * @see #setDestinations
     */
    public boolean hasExplicitDestinations() {
        return this.destinations != null && this.destinations.length > 0;
    }

    /**
     * returns OutputExecuter
     * 
     * @return OutputExecuter
     */
    public Object getOutputExecuter() {
        return outputExecuter;
    }

    /**
     * returns ClusterDefinition
     * 
     * @param output
     * @return
     */
    public ClusterDefinition setOutputExecuter(final Object outputExecuter) {
        this.outputExecuter = outputExecuter;
        return this;
    }

    protected ClusterDefinition setParentApplicationDefinition(final ApplicationDefinition applicationDef) throws IllegalArgumentException {
        if (clusterName == null)
            throw new IllegalArgumentException(
                    "You must set the 'clusterName' when configuring a dempsy cluster for the application: " + String.valueOf(applicationDef));
        final String applicationName = applicationDef.applicationName;
        clusterId = new ClusterId(applicationName, clusterName);
        parent = applicationDef;
        if (destinations != null)
            destinations = Arrays.stream(destinations).map(cid -> cid.applicationName == null ? new ClusterId(applicationName, cid.clusterName) : cid)
                    .toArray(ClusterId[]::new);
        return this;
    }

    public ApplicationDefinition getParentApplicationDefinition() {
        return parent;
    }

    public Object getStatsCollectorFactory() {
        return statsCollectorFactory == null ? parent.getStatsCollectorFactory() : statsCollectorFactory;
    }

    public ClusterDefinition setStatsCollectorFactory(final Object statsCollectorFactory) {
        this.statsCollectorFactory = statsCollectorFactory;
        return this;
    }

    public Object getExecutor() {
        return dempsyExecutor == null ? parent.getExecutor() : dempsyExecutor;
    }

    public ClusterDefinition setExecutor(final Object dempsyExecutor) {
        this.dempsyExecutor = dempsyExecutor;
        return this;
    }

    public ClusterDefinition setMessageProcessor(final MessageProcessor messageProcessor) {
        this.messageProcessorPrototype = messageProcessor;
        return this;
    }

    public MessageProcessor getMessageProcessor() {
        return messageProcessorPrototype;
    }

    public boolean isRouteAdaptorType() {
        return (this.getAdaptor() != null);
    }

    public Adaptor getAdaptor() {
        return adaptor;
    }

    public ClusterDefinition setAdaptor(final Adaptor adaptor) {
        this.adaptor = adaptor;
        return this;
    }

    public boolean isAdaptorDaemon() {
        return adaptorIsDaemon;
    }

    public ClusterDefinition setAdaptorDaemon(final boolean isAdaptorDaemon) {
        this.adaptorIsDaemon = isAdaptorDaemon;
        return this;
    }

    public KeySource<?> getKeySource() {
        return keySource;
    }

    public ClusterDefinition setKeySource(final KeySource<?> keySource) {
        this.keySource = keySource;
        return this;
    }

    public long getEvictionFrequency() {
        return evictionFrequency;
    }

    public ClusterDefinition setEvictionFrequency(final long evictionFrequency) {
        this.evictionFrequency = evictionFrequency;
        return this;
    }

    public TimeUnit getEvictionTimeUnit() {
        return evictionTimeUnit;
    }

    public ClusterDefinition setEvictionTimeUnit(final TimeUnit timeUnit) {
        this.evictionTimeUnit = timeUnit;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("(");
        sb.append(clusterId == null ? "(unknown cluster id)" : String.valueOf(clusterId));
        final Object obj = messageProcessorPrototype == null ? adaptor : messageProcessorPrototype;
        final boolean hasBoth = adaptor != null && messageProcessorPrototype != null;
        sb.append(":").append(obj == null ? "(ERROR: no processor or adaptor)" : obj.getClass().getSimpleName());
        if (hasBoth) // if validate has been called then this can't be true
            sb.append(",").append(adaptor.getClass().getSimpleName()).append("<-ERROR");
        if (hasExplicitDestinations())
            sb.append("|destinations:").append(String.valueOf(destinations));
        sb.append(")");
        return sb.toString();
    }

    public void validate() throws IllegalStateException {
        if (parent == null)
            throw new IllegalStateException("The parent ApplicationDefinition isn't set for the Cluster " +
                    SafeString.valueOf(clusterName) + ". You need to initialize the parent ApplicationDefinition prior to validating");
        if (clusterName == null)
            throw new IllegalStateException("You must set the 'clusterName' when configuring a dempsy cluster for the application.");
        if (messageProcessorPrototype == null && adaptor == null)
            throw new IllegalStateException("A dempsy cluster must contain either an 'adaptor' or a message processor prototype. " +
                    clusterId + " doesn't appear to be configure with either.");
        if (messageProcessorPrototype != null && adaptor != null)
            throw new IllegalStateException("A dempsy cluster must contain either an 'adaptor' or a message processor prototype but not both. " +
                    clusterId + " appears to be configured with both.");

        if (messageProcessorPrototype != null)
            messageProcessorPrototype.validate();

        if (adaptor != null && keySource != null) {
            throw new IllegalStateException("A dempsy cluster can not pre-instantation an adaptor.");
        }
    }
}
