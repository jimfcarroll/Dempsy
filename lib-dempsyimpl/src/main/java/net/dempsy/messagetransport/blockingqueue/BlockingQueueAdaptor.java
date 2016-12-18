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

package net.dempsy.messagetransport.blockingqueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.messagetransport.Destination;
import net.dempsy.messagetransport.Listener;
import net.dempsy.messagetransport.MessageTransportException;
import net.dempsy.messagetransport.OverflowHandler;
import net.dempsy.messagetransport.Receiver;
import net.dempsy.monitoring.StatsCollector;

/**
 * <p>
 * The Message transport default library comes with this BlockingQueue implementation.
 * </p>
 * 
 * <p>
 * This class represents both the MessageTransportSender and the concrete Adaptor (since BlockingQueues don't span process spaces). You need to initialize it with the BlockingQueue to use, as well as the
 * MessageTransportListener to send messages to.
 * </p>
 * 
 * <p>
 * Optionally you can provide it with a name that will be used in the thread that's started to read messages from the queue.
 * </p>
 */
public class BlockingQueueAdaptor implements Runnable, Receiver {
    private static Logger logger = LoggerFactory.getLogger(BlockingQueueAdaptor.class);

    private Thread running;
    private String name;
    private final AtomicReference<Listener> listener = new AtomicReference<Listener>();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private BlockingQueueDestination destination = null;
    private OverflowHandler overflowHandler = null;
    private boolean failFast = false; // this has been the default - since it's equivalent to there being no overflowHandler
    private boolean explicitFailFast = false;

    /**
     * <p>
     * This method starts a background thread that reads messages from the queue and sends them to a registered MessageTransportListener.
     * </p>
     * 
     * <p>
     * This method is tagged with a @PostConstruct but outside of a dependency injection container that's set up to manage default java lifecycle, it needs to be called explicitly.
     * </p>
     * 
     * <p>
     * Alternatively you can manage the threading yourself since a BlockingQueueAdaptor itself is the Runnable that's started.
     * </p>
     * 
     * @throws MessageTransportException
     *             if the BlockingQueue implementation isn't set or if the listener to send the messages to isn't set.
     */
    @Override
    public synchronized void start() throws MessageTransportException {
        // check to see that the overflowHandler and the failFast setting are consistent.
        if (!failFast && overflowHandler != null)
            logger.warn(
                    "BlockingQueueAdaptor is configured with an OverflowHandler that will never be used because it's also configured to NOT 'fail fast' so it will always block waiting for messages to be processed.");

        running = name == null ? new Thread(this) : new Thread(this, name);
        running.setDaemon(true);
        running.start();
    }

    @Override
    public synchronized void stop() {
        shutdown.set(true);
        if (running != null)
            running.interrupt();
    }

    @Override
    public void run() {
        synchronized (this) {
            if (running == null)
                // this is done in case start() wasn't used to start this thread.
                running = Thread.currentThread();
            else if (running != Thread.currentThread()) {
                logger.error(
                        BlockingQueueAdaptor.class.getSimpleName() + " was started in a Runnable more than once. Exiting the additional thread.");
                return;
            }
        }

        while (!shutdown.get()) {
            try {
                final byte[] val = destination.queue.take();
                final Listener curListener = listener.get();

                final boolean messageSuccess = curListener == null ? false : curListener.onMessage(val, failFast);
                if (overflowHandler != null && !messageSuccess)
                    overflowHandler.overflow(val);
            } catch (final InterruptedException ie) {
                // if we were interrupted we're probably stopping.
                if (!shutdown.get())
                    logger.warn("Superfluous interrupt.", ie);
            } catch (final Throwable err) {
                logger.error("Exception while handling message.", err);
            }
        }
    }

    /**
     * Sets the name of this BlockingQueueAdaptor which is currently used in the start method as the name of the Thread that's created.
     * 
     * @param name
     *            is the name to set the BlockingQueueAdaptor to.
     */
    public void setName(final String name) {
        this.name = name;
    }

    /**
     * A BlockingQueueAdaptor requires a MessageTransportListener to be set in order to adapt a client side.
     * 
     * @param listener
     *            is the MessageTransportListener to push messages to when they come in.
     */
    @Override
    public void setListener(final Listener listener) {
        this.listener.set(listener);
    }

    /**
     * When an overflow handler is set the Adaptor indicates that a 'failFast' should happen and any failed message deliveries end up passed to the overflow handler.
     */
    public void setOverflowHandler(final OverflowHandler handler) {
        this.overflowHandler = handler;
        if (!explicitFailFast)
            failFast = (handler != null);
    }

    public void setFailFast(final boolean failFast) {
        explicitFailFast = true;
        this.failFast = failFast;
    }

    public BlockingQueue<byte[]> getQueue() {
        return destination == null ? null : destination.queue;
    }

    public void setQueue(final BlockingQueue<byte[]> queue) {
        this.destination = new BlockingQueueDestination(queue);
    }

    @Override
    public Destination getDestination() {
        return this.destination;
    }

    @Override
    public void setStatsCollector(final StatsCollector statsCollector) {}

}
