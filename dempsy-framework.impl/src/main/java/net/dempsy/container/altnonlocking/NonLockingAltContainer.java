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

package net.dempsy.container.altnonlocking;

import static net.dempsy.util.SafeString.objectDescription;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

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
import net.dempsy.util.StupidHashMap;

/**
 * <p>
 * The {@link NonLockingAltContainer} manages the lifecycle of message processors for the node that it's instantiated in.
 * </p>
 * 
 * The container is simple in that it does no thread management. When it's called it assumes that the transport 
 * has provided the thread that's needed
 */
public class NonLockingAltContainer extends Container {
    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
    private static final int SPIN_TRIES = 100;
    private ClusterStatsCollector statCollector;

    // message key -> instance that handles messages with this key
    // changes to this map will be synchronized; read-only may be concurrent
    private final StupidHashMap<Object, InstanceWrapper> instances = new StupidHashMap<Object, InstanceWrapper>();

    // Scheduler to handle eviction thread.
    private ScheduledExecutorService evictionScheduler;

    private volatile boolean isRunning = true;
    private Set<String> messageTypes;

    private final AtomicBoolean isReady = new AtomicBoolean(false);
    private final AtomicInteger numBeingWorked = new AtomicInteger(0);

    public NonLockingAltContainer() {
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
        if (evictionScheduler != null)
            evictionScheduler.shutdownNow();

        // the following will close up any output executor that might be running
        setOutputCycleConcurrency(-1);
        isRunning = false;
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

    private static class CountedLinkedList<T> {
        private final LinkedList<T> list = new LinkedList<>();
        private int size = 0;

        public T removeFirst() {
            size--;
            return list.removeFirst();
        }

        public boolean add(final T v) {
            final boolean ret = list.add(v);
            if (ret)
                size++;
            return ret;
        }

        public T pushPop(final T toPush) {
            if (size == 0)
                return toPush;
            list.add(toPush);
            return list.removeFirst();
        }
    }

    private static class WorkingQueueHolder {
        final AtomicReference<CountedLinkedList<KeyedMessage>> queue;

        WorkingQueueHolder(final boolean locked) {
            queue = locked ? new AtomicReference<>(null) : new AtomicReference<>(new CountedLinkedList<>());
        }
    }

    private static <T> T setIfAbsent(final AtomicReference<T> ref, final Supplier<T> val) {
        do {
            final T maybeRet = ref.get();
            if (maybeRet != null)
                return maybeRet;

            final T value = val.get();
            if (ref.compareAndSet(null, value))
                return null;
        } while (true);
    }

    protected static class InstanceWrapper {
        private final Object instance;
        private boolean evicted = false;
        private final AtomicReference<WorkingQueueHolder> mailbox = new AtomicReference<>(null);

        public InstanceWrapper(final Object o) {
            this.instance = o;
        }

        // ----------------------------------------------------------------------------
        // Test access
        // ----------------------------------------------------------------------------
        protected Object getInstance() {
            return instance;
        }
    }

    final static class MutRef<X> {
        public X ref;

        public final X set(final X ref) {
            this.ref = ref;
            return ref;
        }
    }

    private <T> T waitFor(final Supplier<T> condition) {
        int counter = SPIN_TRIES;
        do {
            final T ret = condition.get();
            if (ret != null)
                return ret;
            if (counter > 0)
                counter--;
            else Thread.yield();
        } while (isRunning);
        throw new DempsyException("Not running.");
    }

    private CountedLinkedList<KeyedMessage> getQueue(final WorkingQueueHolder wp) {
        return waitFor(() -> wp.queue.getAndSet(null));
    }

    // this is called directly from tests but shouldn't be accessed otherwise.
    @Override
    public void dispatch(final KeyedMessage message, final boolean block) throws IllegalArgumentException, ContainerException {
        if (message == null)
            return; // No. We didn't process the null message

        if (message == null || message.message == null)
            throw new IllegalArgumentException("the container for " + clusterId + " attempted to dispatch null message.");

        if (message.key == null)
            throw new ContainerException("Message " + objectDescription(message.message) + " contains no key.");

        numBeingWorked.incrementAndGet();

        boolean instanceDone = false;
        while (!instanceDone) {
            instanceDone = true;
            final InstanceWrapper wrapper = getInstanceForKey(message.key);

            // wrapper will be null if the activate returns 'false'
            if (wrapper != null) {
                final MutRef<WorkingQueueHolder> mref = new MutRef<>();
                boolean messageDone = false;
                while (!messageDone) {
                    messageDone = true;

                    final WorkingQueueHolder mailbox = setIfAbsent(wrapper.mailbox, () -> mref.set(new WorkingQueueHolder(false)));

                    // if mailbox is null then I got it.
                    if (mailbox == null) {
                        final WorkingQueueHolder box = mref.ref; // can't be null if I got the mailbox
                        final CountedLinkedList<KeyedMessage> q = getQueue(box); // spin until I get the queue
                        KeyedMessage toProcess = q.pushPop(message);
                        box.queue.lazySet(q); // put the queue back

                        while (toProcess != null) {
                            invokeOperation(wrapper.instance, Operation.handle, toProcess);
                            numBeingWorked.getAndDecrement();

                            // get the next message
                            final CountedLinkedList<KeyedMessage> queue = getQueue(box);
                            if (queue.size == 0)
                                // we need to leave the queue out
                                break;

                            toProcess = queue.removeFirst();
                            box.queue.lazySet(queue);
                        }

                        // release the mailbox
                        wrapper.mailbox.set(null);
                    } else {
                        // we didn't get exclusive access so let's see if we can add the message to the mailbox
                        // make one try at putting the message in the mailbox.
                        final CountedLinkedList<KeyedMessage> q = mailbox.queue.getAndSet(null);

                        if (q != null) { // I got it!
                            q.add(message);
                            mailbox.queue.lazySet(q);
                        } else {
                            // see if we're evicted.
                            if (wrapper.evicted) {
                                instanceDone = false;
                                break; // start back at getting the instance.
                            }
                            messageDone = false; // start over from the top.
                        }
                    }
                }
            }
        }

    }

    @Override
    public void evict() {
        if (!prototype.isEvictionSupported() || !isRunning)
            return;

        final MutRef<WorkingQueueHolder> mref = new MutRef<>();
        try (final StatsCollector.TimerContext tctx = statCollector.evictionPassStarted();) {
            // we need to make a copy of the instances in order to make sure
            // the eviction check is done at once.
            final Set<Object> keys = new HashSet<>(instances.size() + 10);
            keys.addAll(instances.keySet());

            while (keys.size() > 0 && instances.size() > 0 && isRunning) {
                for (final Object key : keys) {
                    final InstanceWrapper wrapper = instances.get(key);

                    if (wrapper != null) { // if the MP still exists
                        final WorkingQueueHolder mailbox = setIfAbsent(wrapper.mailbox, () -> mref.set(new WorkingQueueHolder(true)));
                        if (mailbox == null) { // this means I got it.
                            // it was created locked so no one else will be able to drop messages in the mailbox.
                            final Object instance = wrapper.instance;
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
                                wrapper.evicted = true;
                                statCollector.messageProcessorDeleted(key);
                            } else {
                                wrapper.mailbox.set(null); // release the mailbox
                            }
                        } // end - I got the lock. Otherwise it's too busy to evict.
                    } // end if mp exists. Otherwise the mp is already gone.
                }
            }
        }
    }

    private final static AtomicLong evictionThreadNumber = new AtomicLong(0);

    public void startEvictionThread(final long evictionFrequency, final TimeUnit timeUnit) {
        if (0 == evictionFrequency || null == timeUnit) {
            LOGGER.warn("Eviction Thread cannot start with zero frequency or null TimeUnit {} {}", evictionFrequency, timeUnit);
            return;
        }

        if (prototype != null && prototype.isEvictionSupported()) {
            evictionScheduler = Executors.newSingleThreadScheduledExecutor(
                    r -> new Thread(r, NonLockingAltContainer.class.getSimpleName() + "-Eviction-" + evictionThreadNumber.getAndIncrement()));

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

    private static final AtomicLong outputThreadNum = new AtomicLong(0);

    private void setupOutputConcurrency() {
        if (prototype.isOutputSupported() && isRunning) {
            synchronized (lockForExecutorServiceSetter) {
                if (outputConcurrency > 1)
                    outputExecutorService = Executors.newFixedThreadPool(outputConcurrency,
                            r -> new Thread(r, NonLockingAltContainer.class.getSimpleName() + "-Output-" + outputThreadNum.getAndIncrement()));
                else {
                    if (outputExecutorService != null)
                        outputExecutorService.shutdown();
                    outputExecutorService = null;
                }
            }
        }
    }

    // TODO: Output concurrency blocks normal message handling. Need a means of managing this better.
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

        final MutRef<WorkingQueueHolder> mref = new MutRef<>();

        // keep going until all of the outputs have been invoked
        while (toOutput.size() > 0 && isRunning) {
            for (final Iterator<Object> iter = toOutput.iterator(); iter.hasNext();) {
                final Object key = iter.next();

                final InstanceWrapper wrapper = instances.get(key);

                if (wrapper != null) { // if the MP still exists
                    final WorkingQueueHolder mailbox = setIfAbsent(wrapper.mailbox, () -> mref.set(new WorkingQueueHolder(true)));
                    if (mailbox == null) { // this means I got it.
                        // it was created locked so no one else will be able to drop messages in the mailbox.
                        final Semaphore taskSepaphore = taskLock;

                        // This task will release the wrapper's lock.
                        final Runnable task = new Runnable() {

                            @Override
                            public void run() {
                                try {
                                    if (isRunning)
                                        invokeOperation(wrapper.instance, Operation.output, null);
                                } finally {
                                    wrapper.mailbox.set(null); // releases this back to the world

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
                                wrapper.mailbox.set(null); // we never got into the run so we need to release the lock
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
                                wrapper.mailbox.set(null); // we never got into the run so we need to release the lock
                            }
                        } else
                            task.run();

                        iter.remove();

                    } // didn't get the lock
                } else { // end if mp exists. Otherwise the mp is already gone.
                    iter.remove();
                    LOGGER.warn("There was an attempt to output a non-existent Mp for key " + SafeString.objectDescription(key));
                }
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
        try (final StatsCollector.TimerContext tctx = statCollector.outputInvokeStarted()) {
            outputPass();
        }
    }

    // ----------------------------------------------------------------------------
    // Internals
    // ----------------------------------------------------------------------------

    StupidHashMap<Object, Boolean> keysBeingWorked = new StupidHashMap<Object, Boolean>();

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
                instances.putIfAbsent(key, wrapper); // once it goes into the map, we can remove it from the 'being worked' set
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
            List<KeyedMessageWithType> result;
            try {
                statCollector.messageDispatched(message);
                result = op == Operation.output ? prototype.invokeOutput(instance)
                        : prototype.invoke(instance, message);
                statCollector.messageProcessed(message);
            } catch (final ContainerException e) {
                result = null;
                LOGGER.warn("the container for " + clusterId + " failed to invoke " + op + " on the message processor " +
                        SafeString.valueOf(prototype) + (op == Operation.handle ? (" with " + objectDescription(message)) : ""), e);
                statCollector.messageFailed(false);
            }
            // this is an exception thrown as a result of the reflected call having an illegal argument.
            // This should actually be impossible since the container itself manages the calling.
            catch (final IllegalArgumentException e) {
                result = null;
                LOGGER.error("the container for " + clusterId + " failed when trying to invoke " + op + " on " + objectDescription(instance) +
                        " due to a declaration problem. Are you sure the method takes the type being routed to it? If this is an output operation are you sure the output method doesn't take any arguments?",
                        e);
                statCollector.messageFailed(true);
            }
            // The app threw an exception.
            catch (final DempsyException e) {
                result = null;
                LOGGER.warn("the container for " + clusterId + " failed when trying to invoke " + op + " on " + objectDescription(instance) +
                        " because an exception was thrown by the Message Processeor itself", e);
                statCollector.messageFailed(true);
            }
            // RuntimeExceptions bookeeping
            catch (final RuntimeException e) {
                result = null;
                LOGGER.error("the container for " + clusterId + " failed when trying to invoke " + op + " on " + objectDescription(instance) +
                        " due to an unknown exception.", e);
                statCollector.messageFailed(false);

                if (op == Operation.handle)
                    throw e;
            }
            if (result != null) {
                try {
                    dispatcher.dispatch(result);
                } catch (final Exception de) {
                    LOGGER.warn("Failed on subsequent dispatch of " + result + ": " + de.getLocalizedMessage());
                }
            }
        }
    }

}
