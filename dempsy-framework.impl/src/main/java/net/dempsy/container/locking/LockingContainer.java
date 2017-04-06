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

package net.dempsy.container.locking;

import static net.dempsy.util.SafeString.objectDescription;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.DempsyException;
import net.dempsy.Infrastructure;
import net.dempsy.container.Container;
import net.dempsy.container.ContainerException;
import net.dempsy.messages.KeyedMessage;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.messages.MessageProcessorLifecycle;
import net.dempsy.monitoring.ClusterStatsCollector;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.util.SafeString;

/**
 * <p>
 * The {@link LockingContainer} manages the lifecycle of message processors for the node that it's instantiated in.
 * </p>
 * 
 * The container is simple in that it does no thread management. When it's called it assumes that the transport 
 * has provided the thread that's needed
 */
public class LockingContainer extends Container {
    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    private ClusterStatsCollector statCollector;

    // message key -> instance that handles messages with this key
    // changes to this map will be synchronized; read-only may be concurrent
    private final ConcurrentHashMap<Object, InstanceWrapper> instances = new ConcurrentHashMap<Object, InstanceWrapper>();

    // Scheduler to handle eviction thread.
    private ScheduledExecutorService evictionScheduler;

    private volatile boolean isRunning = true;
    private Set<String> messageTypes;

    private final AtomicBoolean isReady = new AtomicBoolean(false);
    private final AtomicInteger numBeingWorked = new AtomicInteger(0);

    public LockingContainer() {
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
        setOutputCycleConcurrency(-1);
    }

    @Override
    public void start(final Infrastructure infra) {
        statCollector = infra.getClusterStatsCollector(clusterId);

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

    @Override
    public int getMessageWorkingCount() {
        return numBeingWorked.get();
    }

    // ----------------------------------------------------------------------------
    // Test Hooks
    // ----------------------------------------------------------------------------

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    protected static class InstanceWrapper {
        private final Object instance;
        private final Semaphore lock = new Semaphore(1, true); // basically a mutex
        private boolean evicted = false;

        /**
         * DO NOT CALL THIS WITH NULL OR THE LOCKING LOGIC WONT WORK
         */
        public InstanceWrapper(final Object o) {
            this.instance = o;
        }

        /**
         * MAKE SURE YOU USE A FINALLY CLAUSE TO RELEASE THE LOCK.
         * 
         * @param block
         *            - whether or not to wait for the lock.
         * @return the instance if the lock was acquired. null otherwise.
         */
        public Object getExclusive(final boolean block) {
            if (block) {
                boolean gotLock = false;
                while (!gotLock) {
                    try {
                        lock.acquire();
                        gotLock = true;
                    } catch (final InterruptedException e) {}
                }
            } else {
                if (!lock.tryAcquire())
                    return null;
            }

            // if we got here we have the lock
            return instance;
        }

        /**
         * MAKE SURE YOU USE A FINALLY CLAUSE TO RELEASE THE LOCK. MAKE SURE YOU OWN THE LOCK IF YOU UNLOCK IT.
         */
        public void releaseLock() {
            lock.release();
        }

        public boolean tryLock() {
            return lock.tryAcquire();
        }

        // /**
        // * This will set the instance reference to null. MAKE SURE
        // * YOU OWN THE LOCK BEFORE CALLING.
        // */
        // public void markPassivated() { instance = null; }
        //
        // /**
        // * This will tell you if the instance reference is null. MAKE SURE
        // * YOU OWN THE LOCK BEFORE CALLING.
        // */
        // public boolean isPassivated() { return instance == null; }

        /**
         * This will prevent further operations on this instance. MAKE SURE YOU OWN THE LOCK BEFORE CALLING.
         */
        public void markEvicted() {
            evicted = true;
        }

        /**
         * Flag to indicate this instance has been evicted and no further operations should be enacted. THIS SHOULDN'T BE CALLED WITHOUT HOLDING THE LOCK.
         */
        public boolean isEvicted() {
            return evicted;
        }

        // ----------------------------------------------------------------------------
        // Test access
        // ----------------------------------------------------------------------------
        protected Object getInstance() {
            return instance;
        }
    }

    // this is called directly from tests but shouldn't be accessed otherwise.
    @Override
    public void dispatch(final KeyedMessage message, final boolean block) throws IllegalArgumentException, ContainerException {
        if (message == null)
            return; // No. We didn't process the null message

        boolean evictedAndBlocking;

        if (message == null || message.message == null)
            throw new IllegalArgumentException("the container for " + clusterId + " attempted to dispatch null message.");

        if (message.key == null)
            throw new ContainerException("Message " + objectDescription(message.message) + " contains no key.");

        try {
            numBeingWorked.incrementAndGet();

            do {
                evictedAndBlocking = false;

                final InstanceWrapper wrapper = getInstanceForKey(message.key);

                // wrapper will be null if the activate returns 'false'
                if (wrapper != null) {
                    final Object instance = wrapper.getExclusive(block);

                    if (instance != null) { // null indicates we didn't get the lock
                        try {
                            if (wrapper.isEvicted()) {
                                // if we're not blocking then we need to just return a failure. Otherwise we want to try again
                                // because eventually the current Mp will be passivated and removed from the container and
                                // a subsequent call to getInstanceForDispatch will create a new one.
                                if (block) {
                                    Thread.yield();
                                    evictedAndBlocking = true; // we're going to try again.
                                } else { // otherwise it's just like we couldn't get the lock. The Mp is busy being killed off.
                                    if (LOGGER.isTraceEnabled())
                                        LOGGER.trace("the container for " + clusterId + " failed handle message due to evicted Mp "
                                                + SafeString.valueOf(prototype));

                                    statCollector.messageCollision(message);
                                }
                            } else
                                invokeOperation(wrapper.getInstance(), Operation.handle, message);
                        } finally {
                            wrapper.releaseLock();
                        }
                    } else { // ... we didn't get the lock
                        if (LOGGER.isTraceEnabled())
                            LOGGER.trace("the container for " + clusterId + " failed to obtain lock on " + SafeString.valueOf(prototype));
                        statCollector.messageCollision(message);
                    }
                } else {
                    // if we got here then the activate on the Mp explicitly returned 'false'
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("the container for " + clusterId + " failed to activate the Mp for " + SafeString.valueOf(prototype));
                    // we consider this "processed"
                    break; // leave the do/while loop
                }
            } while (evictedAndBlocking);

        } finally {
            numBeingWorked.decrementAndGet();
        }
    }

    @Override
    public void evict() {
        doEvict(new EvictCheck() {
            @Override
            public boolean shouldEvict(final Object key, final InstanceWrapper wrapper) {
                final Object instance = wrapper.getInstance();
                try {
                    return prototype.invokeEvictable(instance);
                } catch (final DempsyException e) {
                    LOGGER.warn("Checking the eviction status/passivating of the Mp " + SafeString.objectDescription(instance) +
                            " resulted in an exception.", e.getCause());
                }
                return false;
            }

            @Override
            public boolean isGenerallyEvitable() {
                return prototype.isEvictionSupported();
            }

            @Override
            public boolean shouldStopEvicting() {
                return false;
            }
        });
    }

    private interface EvictCheck {
        boolean isGenerallyEvitable();

        boolean shouldEvict(Object key, InstanceWrapper wrapper);

        boolean shouldStopEvicting();
    }

    private void doEvict(final EvictCheck check) {
        if (!check.isGenerallyEvitable() || !isRunning)
            return;

        try (final StatsCollector.TimerContext tctx = statCollector.evictionPassStarted()) {

            // we need to make a copy of the instances in order to make sure
            // the eviction check is done at once.
            final Map<Object, InstanceWrapper> instancesToEvict = new HashMap<Object, InstanceWrapper>(instances.size() + 10);
            instancesToEvict.putAll(instances);

            while (instancesToEvict.size() > 0 && instances.size() > 0 && isRunning && !check.shouldStopEvicting()) {
                // store off anything that passes for later removal. This is to avoid a
                // ConcurrentModificationException.
                final Set<Object> keysToRemove = new HashSet<Object>();

                for (final Map.Entry<Object, InstanceWrapper> entry : instancesToEvict.entrySet()) {
                    if (check.shouldStopEvicting())
                        break;

                    final Object key = entry.getKey();
                    final InstanceWrapper wrapper = entry.getValue();
                    boolean gotLock = false;
                    try {
                        gotLock = wrapper.tryLock();
                        if (gotLock) {

                            // since we got here we're done with this instance,
                            // so add it's key to the list of keys we plan don't
                            // need to return to.
                            keysToRemove.add(key);

                            boolean removeInstance = false;

                            try {
                                if (check.shouldEvict(key, wrapper)) {
                                    removeInstance = true;
                                    wrapper.markEvicted();
                                    prototype.passivate(wrapper.getInstance());
                                    // wrapper.markPassivated();
                                }
                            } catch (final Throwable e) {
                                Object instance = null;
                                try {
                                    instance = wrapper.getInstance();
                                } catch (final Throwable th) {} // not sure why this would ever happen
                                LOGGER.warn("Checking the eviction status/passivating of the Mp "
                                        + SafeString.objectDescription(instance == null ? wrapper : instance) +
                                        " resulted in an exception.", e);
                            }

                            // even if passivate throws an exception, if the eviction check returned 'true' then
                            // we need to remove the instance.
                            if (removeInstance) {
                                instances.remove(key);
                                statCollector.messageProcessorDeleted(key);
                            }
                        }
                    } finally {
                        if (gotLock)
                            wrapper.releaseLock();
                    }

                }

                // now clean up everything we managed to get hold of
                for (final Object key : keysToRemove)
                    instancesToEvict.remove(key);

            }
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

    @Override
    public void setOutputCycleConcurrency(final int concurrency) {
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
        final LinkedList<InstanceWrapper> toOutput = new LinkedList<InstanceWrapper>(instances.values());

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
            for (final Iterator<InstanceWrapper> iter = toOutput.iterator(); iter.hasNext();) {
                final InstanceWrapper wrapper = iter.next();
                boolean gotLock = false;

                gotLock = wrapper.tryLock();

                if (gotLock) {
                    // If we've been evicted then we're on our way out
                    // so don't do anything else with this.
                    if (wrapper.isEvicted()) {
                        iter.remove();
                        wrapper.releaseLock();
                        continue;
                    }

                    final Object instance = wrapper.getInstance(); // only called while holding the lock
                    final Semaphore taskSepaphore = taskLock;

                    // This task will release the wrapper's lock.
                    final Runnable task = new Runnable() {
                        @Override
                        public void run() {
                            try {
                                if (isRunning && !wrapper.isEvicted())
                                    invokeOperation(instance, Operation.output, null);
                            } finally {
                                wrapper.releaseLock();

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
                            // this may happen because of a race condition between the
                            taskSepaphore.release();
                            wrapper.releaseLock(); // we never got into the run so we need to release the lock
                        } catch (final InterruptedException e) {
                            // this can happen while blocked in the semaphore.acquire.
                            // if we're no longer running we should just get out
                            // of here.
                            //
                            // Not releasing the taskSepaphore assumes the acquire never executed.
                            // if (since) the acquire never executed we also need to release the
                            // wrapper lock or that Mp will never be usable again.
                            wrapper.releaseLock(); // we never got into the run so we need to release the lock
                        }
                    } else
                        task.run();

                    iter.remove();
                } // end if we got the lock
            } // end loop over every Mp
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
        try (final StatsCollector.TimerContext tctx = statCollector.outputInvokeStarted();) {
            outputPass();
        }
    }

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    ConcurrentHashMap<Object, Boolean> keysBeingWorked = new ConcurrentHashMap<Object, Boolean>();

    /**
     * This is required to return non null or throw a ContainerException
     */
    InstanceWrapper getInstanceForKey(final Object key) throws ContainerException {
        // common case has "no" contention
        InstanceWrapper wrapper = instances.get(key);
        if (wrapper != null)
            return wrapper;

        // otherwise we will be working to get one.
        final Boolean tmplock = new Boolean(true);
        Boolean lock = keysBeingWorked.putIfAbsent(key, tmplock);
        if (lock == null)
            lock = tmplock;

        // otherwise we'll do an atomic check-and-update
        synchronized (lock) {
            wrapper = instances.get(key); // double checked lock?????
            if (wrapper != null)
                return wrapper;

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
                wrapper = new InstanceWrapper(instance); // null check above.
                instances.put(key, wrapper); // once it goes into the map, we can remove it from the 'being worked' set
                keysBeingWorked.remove(key); // remove it from the keysBeingWorked since any subsequent call will get
                // the newly added one.
                statCollector.messageProcessorCreated(key);
            }
            return wrapper;
        }
    }

    public enum Operation {
        handle,
        output
    };

    /**
     * helper method to invoke an operation (handle a message or run output) handling all of hte exceptions and forwarding any results.
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

                if (op == Operation.handle)
                    throw e;
            }
        }
    }

}
