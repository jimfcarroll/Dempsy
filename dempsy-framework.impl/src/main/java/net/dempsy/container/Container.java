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

package net.dempsy.container;

import net.dempsy.Service;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.Dispatcher;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.MessageProcessorLifecycle;

/**
 * <p>
 * The {@link Container} manages the lifecycle of message processors for the node that it's instantiated in.
 * </p>
 * 
 * The container is simple in that it does no thread management. When it's called it assumes that the transport 
 * has provided the thread that's needed
 */
public abstract class Container implements Service {

    protected Dispatcher dispatcher;

    // The ClusterId is set for the sake of error messages.
    protected ClusterId clusterId;

    protected MessageProcessorLifecycle<Object> prototype;

    // ----------------------------------------------------------------------------
    // Configuration
    // ----------------------------------------------------------------------------

    public Container setDispatcher(final Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
        return this;
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    @SuppressWarnings("unchecked")
    public Container setMessageProcessor(final MessageProcessorLifecycle<?> prototype) {
        if (prototype == null)
            throw new IllegalArgumentException("The container for cluster(" + clusterId + ") cannot be supplied a null MessageProcessor");

        this.prototype = (MessageProcessorLifecycle<Object>) prototype;

        return this;
    }

    public Container setClusterId(final ClusterId clusterId) {
        if (clusterId == null)
            throw new IllegalArgumentException("The container must have a cluster id");

        this.clusterId = clusterId;

        return this;
    }

    public ClusterId getClusterId() {
        return clusterId;
    }

    // ----------------------------------------------------------------------------
    // Monitoring / Management
    // ----------------------------------------------------------------------------

    // ----------------------------------------------------------------------------
    // Operation
    // ----------------------------------------------------------------------------

    // ----------------------------------------------------------------------------
    // Monitoring and Management
    // ----------------------------------------------------------------------------

    /**
     * Returns the number of message processors controlled by this manager.
     */
    public abstract int getProcessorCount();

    /**
     * The number of messages the container is currently working on. This may not match the 
     * stats collector's numberOfInFlight messages which is a measurement of how many messages
     * are currently being handled by MPs. This represents how many messages have been given
     * dispatched to the container that are yet being processed. It includes internal queuing
     * in the container as well as messages actually in Mp handlers.
     */
    public abstract int getMessageWorkingCount();

    // ----------------------------------------------------------------------------
    // Test Hooks
    // ----------------------------------------------------------------------------

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    // this is called directly from tests but shouldn't be accessed otherwise.
    public abstract void dispatch(final KeyedMessage message, final boolean block) throws IllegalArgumentException, ContainerException;

    public abstract void evict();

    // This method MUST NOT THROW
    public abstract void outputPass();

    public abstract void setOutputCycleConcurrency(final int concurrency);

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    protected void validate() {
        if (clusterId == null)
            throw new IllegalStateException("The container must have a cluster id");

        if (prototype == null)
            throw new IllegalStateException("The container for \"" + clusterId + "\" cannot be supplied a null MessageProcessor");

        if (dispatcher == null)
            throw new IllegalStateException("The container for cluster \"" + clusterId + "\" never had the dispatcher set on it.");

    }

}
