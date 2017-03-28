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

package net.dempsy.container.nonlocking;

import static net.dempsy.util.SafeString.objectDescription;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.DempsyException;
import net.dempsy.Infrastructure;
import net.dempsy.container.Container;
import net.dempsy.container.ContainerException;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.messages.MessageProcessorLifecycle;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.util.SafeString;

/**
 * <p>
 * The {@link NonLockingContainer} manages the lifecycle of message processors for the node that it's instantiated in.
 * </p>
 * 
 * The container is simple in that it does no thread management. When it's called it assumes that the transport 
 * has provided the thread that's needed
 */
public class NonLockingContainer extends Container {
    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    private StatsCollector statCollector;

    // message key -> instance that handles messages with this key
    // changes to this map will be synchronized; read-only may be concurrent
    private final ConcurrentHashMap<Object, WorkingPlaceholder> working = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Object, Object> instances = new ConcurrentHashMap<>();

    // Scheduler to handle eviction thread.
    private ScheduledExecutorService evictionScheduler;

    private volatile boolean isRunning = true;
    private Set<String> messageTypes;

    private final AtomicBoolean isReady = new AtomicBoolean(false);
    private final AtomicLong numBeingWorked = new AtomicLong(0L);

    private static class WorkingQueueHolder {
        List<KeyedMessage> queue = null;
    }

    protected static class WorkingPlaceholder {
        AtomicReference<WorkingQueueHolder> mailbox = new AtomicReference<>(new WorkingQueueHolder());
    }

    public NonLockingContainer() {
        if (outputConcurrency > 0)
            setupOutputConcurrency();
    }

    // ----------------------------------------------------------------------------
    // Configuration
    // ----------------------------------------------------------------------------

    // ----------------------------------------------------------------------------
    // Monitoring / Management
    // ----------------------------------------------------------------------------

    // ----------------------------------------------------------------------------
    // Operation
    // ----------------------------------------------------------------------------

    @Override
    public void stop() {
        isRunning = false;
        if (evictionScheduler != null)
            evictionScheduler.shutdownNow();

        // the following will close up any output executor that might be running
        setConcurrency(-1);
    }

    @Override
    public void start(final Infrastructure infra) {
        statCollector = infra.getStatsCollector();

        validate();

        prototype.start(clusterId);

        messageTypes = ((MessageProcessorLifecycle<?>) prototype).messagesTypesHandled();
        if (messageTypes == null || messageTypes.size() == 0)
            throw new ContainerException("The cluster " + clusterId + " appears to have a MessageProcessor with no messageTypes defined.");

        isReady.set(true);
    }

    @Override
    public boolean isReady() {
        return isReady.get();
    }

    @Override
    protected void validate() {
        super.validate();

        if (statCollector == null)
            throw new IllegalStateException("The container must have a " + StatsCollector.class.getSimpleName() + " id");
    }

    // ----------------------------------------------------------------------------
    // Monitoring and Management
    // ----------------------------------------------------------------------------

    /**
     * Returns the number of message processors controlled by this manager.
     */
    @Override
    public int getProcessorCount() {
        return instances.size();
    }

    // ----------------------------------------------------------------------------
    // Test Hooks
    // ----------------------------------------------------------------------------

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    // this is called directly from tests but shouldn't be accessed otherwise.

    private Object createAndActivate(final Object key) throws ContainerException {
        Object instance = null;
        try {
            instance = prototype.newInstance();
        } catch (final DempsyException e) {
            throw new ContainerException("the container for " + clusterId + " failed to create a new instance of " +
                    SafeString.valueOf(prototype) + " for the key " + SafeString.objectDescription(key) +
                    " because the clone method threw an exception.", e);
        } catch (final RuntimeException e) {
            throw new ContainerException("the container for " + clusterId + " failed to create a new instance of " +
                    SafeString.valueOf(prototype) + " for the key " + SafeString.objectDescription(key) +
                    " because the clone invocation resulted in an unknown exception.", e);
        }

        if (instance == null)
            throw new ContainerException("the container for " + clusterId + " failed to create a new instance of " +
                    SafeString.valueOf(prototype) + " for the key " + SafeString.objectDescription(key) +
                    ". The value returned from the clone call appears to be null.");

        // activate
        final byte[] data = null;
        if (LOGGER.isTraceEnabled())
            LOGGER.trace("the container for " + clusterId + " is activating instance " + String.valueOf(instance)
                    + " with " + ((data != null) ? data.length : 0) + " bytes of data"
                    + " via " + SafeString.valueOf(prototype));

        boolean activateSuccessful = false;
        try {
            prototype.activate(instance, key, data);
            activateSuccessful = true;
        } catch (final IllegalArgumentException e) {
            throw new ContainerException(
                    "the container for " + clusterId + " failed to invoke the activate method of " + SafeString.valueOf(prototype) +
                            ". Is it declared to take a byte[]?",
                    e);
        } catch (final DempsyException e) {
            throw new ContainerException(
                    "the container for " + clusterId + " failed to invoke the activate method of " + SafeString.valueOf(prototype) +
                            ". Is the active method accessible - the class is public and the method is public?",
                    e);
        } catch (final RuntimeException e) {
            throw new ContainerException(
                    "the container for " + clusterId + " failed to invoke the activate method of " + SafeString.valueOf(prototype) +
                            " because of an unknown exception.",
                    e);
        }

        if (activateSuccessful) {
            // we only want to create a wrapper and place the instance into the container
            // if the instance activated correctly. If we got here then the above try block
            // must have been successful.
            instances.put(key, instance); // once it goes into the map, we can remove it from the 'being worked' set
            // the newly added one.
            statCollector.messageProcessorCreated(key);
        }
        return instance;
    }

    private WorkingQueueHolder getQueue(final WorkingPlaceholder wp) {
        // Spin on the queue
        WorkingQueueHolder mailbox = null;
        for (boolean mine = false; !mine;) {
            mailbox = wp.mailbox.getAndSet(null);

            if (mailbox == null)
                Thread.yield();
            else
                mine = true;
        }

        return mailbox;
    }

    private void drainPendingMessages(final WorkingPlaceholder wp, final boolean mpFailure) {
        final WorkingQueueHolder mailbox = getQueue(wp);
        if (mailbox.queue != null) {
            mailbox.queue.forEach(m -> {
                LOGGER.debug("Failed to process message with key " + SafeString.objectDescription(m.key));
                statCollector.messageFailed(mpFailure);
            });
        }
    }

    @Override
    public void dispatch(final KeyedMessage message, final boolean block) throws IllegalArgumentException, ContainerException {
        statCollector.messageReceived(message);
        if (message == null)
            return; // No. We didn't process the null message

        if (message.message == null)
            throw new IllegalArgumentException("the container for " + clusterId + " attempted to dispatch null message.");

        if (message.key == null)
            throw new ContainerException("Message " + objectDescription(message.message) + " contains no key.");

        final Object key = message.key;

        boolean keepTrying = true;
        while (keepTrying) {
            final WorkingPlaceholder wp = new WorkingPlaceholder();
            final WorkingPlaceholder alreadyThere = working.putIfAbsent(key, wp);

            if (alreadyThere == null) { // we're it!
                keepTrying = false; // we're not going to keep trying.
                try { // if we don't get the WorkingPlaceholder out of the working map then that Mp will forever be lost.
                    numBeingWorked.incrementAndGet();

                    Object instance = instances.get(key);
                    if (instance == null) {
                        try {
                            // this can throw ... TODO: handle instantiation failure.
                            instance = createAndActivate(key);
                        } catch (final RuntimeException e) {
                            drainPendingMessages(wp, true);
                            instance = null;
                        }
                    }

                    KeyedMessage curMessage = message;
                    while (curMessage != null) {
                        if (instance != null) { // if it's null then activation failed.
                            invokeOperation(instance, Operation.handle, curMessage);
                            numBeingWorked.decrementAndGet(); // decrement the initial increment.
                        }

                        // work off the queue.
                        final WorkingQueueHolder mailbox = getQueue(wp); // spin until I have it.
                        if (mailbox.queue != null && mailbox.queue.size() > 0) {
                            curMessage = mailbox.queue.remove(0);
                            // curMessage CAN'T be NULL!!!!

                            // put it back - releasing the lock
                            wp.mailbox.set(mailbox);
                        } else {
                            curMessage = null;
                            // DON'T put the queue back ... it will stay empty until we're completely done.
                        }
                    }
                } finally {
                    working.remove(key);
                }
            } else { // ... we didn't get the lock
                if (!block) { // blocking means no collisions allowed.
                    if (LOGGER.isTraceEnabled())
                        LOGGER.trace("the container for " + clusterId + " failed to obtain lock on " + SafeString.valueOf(prototype));
                    statCollector.messageCollision(message);
                    keepTrying = false;
                } else {
                    // try and get the queue.
                    final WorkingQueueHolder mailbox = alreadyThere.mailbox.getAndSet(null);

                    if (mailbox != null) { // we got the queue!
                        try {
                            keepTrying = false;
                            numBeingWorked.incrementAndGet();
                            if (mailbox.queue == null)
                                mailbox.queue = new ArrayList<>(4);
                            mailbox.queue.add(message);
                        } finally {
                            // put it back - releasing the lock
                            alreadyThere.mailbox.set(mailbox);
                        }
                    } else {
                        // we failed to get the queue ... maybe we'll have better luck next time.
                    }
                }
            }
        }
    }

    @Override
    public void evict() {
        if (!prototype.isEvictionSupported() || !isRunning)
            return;

        StatsCollector.TimerContext tctx = null;
        try {
            tctx = statCollector.evictionPassStarted();

            // we need to make a copy of the instances in order to make sure
            // the eviction check is done at once.
            final Set<Object> keys = new HashSet<>(instances.size() + 10);
            keys.addAll(instances.keySet());

            while (keys.size() > 0 && instances.size() > 0 && isRunning) {
                for (final Object key : keys) {

                    final WorkingPlaceholder wp = new WorkingPlaceholder();
                    // we're going to hold up all incomming message to this mp
                    wp.mailbox.getAndSet(null); // this blocks other threads from
                                                // dropping messages in the mailbox

                    final WorkingPlaceholder alreadyThere = working.putIfAbsent(key, wp); // try to get a lock

                    if (alreadyThere == null) { // we got it the lock
                        try {
                            final Object instance = instances.get(key);

                            if (instance != null) {
                                boolean evictMe;
                                try {
                                    evictMe = prototype.invokeEvictable(instance);
                                } catch (final RuntimeException e) {
                                    LOGGER.warn("Checking the eviction status/passivating of the Mp " + SafeString.objectDescription(instance) +
                                            " resulted in an exception.", e.getCause());
                                    evictMe = false;
                                }

                                if (evictMe) {
                                    try {
                                        prototype.passivate(instance);
                                    } catch (final Throwable e) {
                                        LOGGER.warn("Checking the eviction status/passivating of the Mp "
                                                + SafeString.objectDescription(instance) + " resulted in an exception.", e);
                                    }

                                    // even if passivate throws an exception, if the eviction check returned 'true' then
                                    // we need to remove the instance.
                                    instances.remove(key);
                                    statCollector.messageProcessorDeleted(key);
                                }
                            } else {
                                LOGGER.warn("There was an attempt to evict a non-existent Mp for key " + SafeString.objectDescription(key));
                            }
                        } finally {
                            working.remove(key); // releases this back to the world
                        }
                    }
                }
            }
        } finally {
            if (tctx != null)
                tctx.stop();
        }
    }

    public void startEvictionThread(final long evictionFrequency, final TimeUnit timeUnit) {
        if (0 == evictionFrequency || null == timeUnit) {
            LOGGER.warn("Eviction Thread cannot start with zero frequency or null TimeUnit {} {}", evictionFrequency, timeUnit);
            return;
        }

        if (prototype != null && prototype.isEvictionSupported()) {
            evictionScheduler = Executors.newSingleThreadScheduledExecutor();
            evictionScheduler.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    evict();
                }
            }, evictionFrequency, evictionFrequency, timeUnit);
        }
    }

    private ExecutorService outputExecutorService = null;
    private int outputConcurrency = -1;
    private final Object lockForExecutorServiceSetter = new Object();

    public void setConcurrency(final int concurrency) {
        synchronized (lockForExecutorServiceSetter) {
            outputConcurrency = concurrency;
            if (prototype != null) // otherwise this isn't initialized yet
                setupOutputConcurrency();
        }
    }

    private void setupOutputConcurrency() {
        if (prototype.isOutputSupported() && isRunning) {
            synchronized (lockForExecutorServiceSetter) {
                if (outputConcurrency > 1)
                    outputExecutorService = Executors.newFixedThreadPool(outputConcurrency);
                else {
                    if (outputExecutorService != null)
                        outputExecutorService.shutdown();
                    outputExecutorService = null;
                }
            }
        }
    }

    // This method MUST NOT THROW
    @Override
    public void outputPass() {
        if (!prototype.isOutputSupported())
            return;

        // take a snapshot of the current container state.
        final LinkedList<Object> toOutput = new LinkedList<Object>(instances.keySet());

        Executor executorService = null;
        Semaphore taskLock = null;
        synchronized (lockForExecutorServiceSetter) {
            executorService = outputExecutorService;
            if (executorService != null)
                taskLock = new Semaphore(outputConcurrency);
        }

        // This keeps track of the number of concurrently running
        // output tasks so that this method can wait until they're
        // all done to return.
        //
        // It's also used as a condition variable signaling on its
        // own state changes.
        final AtomicLong numExecutingOutputs = new AtomicLong(0);

        // keep going until all of the outputs have been invoked
        while (toOutput.size() > 0 && isRunning) {
            for (final Iterator<Object> iter = toOutput.iterator(); iter.hasNext();) {
                final Object key = iter.next();

                final WorkingPlaceholder wp = new WorkingPlaceholder();
                // we're going to hold up all incomming message to this mp
                wp.mailbox.getAndSet(null); // this blocks other threads from
                                            // dropping messages in the mailbox

                final WorkingPlaceholder alreadyThere = working.putIfAbsent(key, wp); // try to get a lock
                if (alreadyThere == null) { // we got it the lock
                    final Object instance = instances.get(key);

                    if (instance != null) {
                        final Semaphore taskSepaphore = taskLock;

                        // This task will release the wrapper's lock.
                        final Runnable task = new Runnable() {

                            @Override
                            public void run() {
                                try {
                                    if (isRunning)
                                        invokeOperation(instance, Operation.output, null);
                                } finally {
                                    working.remove(key); // releases this back to the world

                                    // this signals that we're done.
                                    synchronized (numExecutingOutputs) {
                                        numExecutingOutputs.decrementAndGet();
                                        numExecutingOutputs.notifyAll();
                                    }
                                    if (taskSepaphore != null)
                                        taskSepaphore.release();
                                }
                            }
                        };

                        synchronized (numExecutingOutputs) {
                            numExecutingOutputs.incrementAndGet();
                        }

                        if (executorService != null) {
                            try {
                                taskSepaphore.acquire();
                                executorService.execute(task);
                            } catch (final RejectedExecutionException e) {
                                working.remove(key); // we never got into the run so we need to release the lock
                                // this may happen because of a race condition between the
                                taskSepaphore.release();
                            } catch (final InterruptedException e) {
                                // this can happen while blocked in the semaphore.acquire.
                                // if we're no longer running we should just get out
                                // of here.
                                //
                                // Not releasing the taskSepaphore assumes the acquire never executed.
                                // if (since) the acquire never executed we also need to release the
                                // wrapper lock or that Mp will never be usable again.
                                working.remove(key); // we never got into the run so we need to release the lock
                            }
                        } else
                            task.run();

                        iter.remove();

                    } else {
                        working.remove(key);
                        LOGGER.warn("There was an attempt to evict a non-existent Mp for key " + SafeString.objectDescription(key));
                    }
                } // didn't get the lock
            } // loop over every mp
        } // end while there are still Mps that haven't had output invoked.

        // =======================================================
        // now make sure all of the running tasks have completed
        synchronized (numExecutingOutputs) {
            while (numExecutingOutputs.get() > 0) {
                try {
                    numExecutingOutputs.wait();
                } catch (final InterruptedException e) {
                    // if we were interupted for a shutdown then just stop
                    // waiting for all of the threads to finish
                    if (!isRunning)
                        break;
                    // otherwise continue checking.
                }
            }
        }
        // =======================================================

    }

    public void invokeOutput() {
        final StatsCollector.TimerContext tctx = statCollector.outputInvokeStarted();
        try {
            outputPass();
        } finally {
            tctx.stop();
        }
    }

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    public enum Operation {
        handle,
        output
    };

    /**
     * helper method to invoke an operation (handle a message or run output) handling all of the exceptions and forwarding any results.
     */
    private void invokeOperation(final Object instance, final Operation op, final KeyedMessage message) {
        if (instance != null) { // possibly passivated ...
            try {
                statCollector.messageDispatched(message);
                final List<KeyedMessageWithType> result = op == Operation.output ? prototype.invokeOutput(instance)
                        : prototype.invoke(instance, message);
                statCollector.messageProcessed(message);
                if (result != null)
                    dispatcher.dispatch(result);
            } catch (final ContainerException e) {
                LOGGER.warn("the container for " + clusterId + " failed to invoke " + op + " on the message processor " +
                        SafeString.valueOf(prototype) + (op == Operation.handle ? (" with " + objectDescription(message)) : ""), e);
                statCollector.messageFailed(false);
            }
            // this is an exception thrown as a result of the reflected call having an illegal argument.
            // This should actually be impossible since the container itself manages the calling.
            catch (final IllegalArgumentException e) {
                LOGGER.error("the container for " + clusterId + " failed when trying to invoke " + op + " on " + objectDescription(instance) +
                        " due to a declaration problem. Are you sure the method takes the type being routed to it? If this is an output operation are you sure the output method doesn't take any arguments?",
                        e);
                statCollector.messageFailed(true);
            }
            // The app threw an exception.
            catch (final DempsyException e) {
                LOGGER.warn("the container for " + clusterId + " failed when trying to invoke " + op + " on " + objectDescription(instance) +
                        " because an exception was thrown by the Message Processeor itself.", e.getCause());
                statCollector.messageFailed(true);
            }
            // RuntimeExceptions bookeeping
            catch (final RuntimeException e) {
                LOGGER.error("the container for " + clusterId + " failed when trying to invoke " + op + " on " + objectDescription(instance) +
                        " due to an unknown exception.", e);
                statCollector.messageFailed(false);
            }
        }
    }

}
