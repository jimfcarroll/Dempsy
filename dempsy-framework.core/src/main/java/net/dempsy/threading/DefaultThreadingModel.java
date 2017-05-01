package net.dempsy.threading;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class DefaultThreadingModel implements ThreadingModel {
    private static final int minNumThreads = 4;

    public static final String CONFIG_KEY_MAX_PENDING = "max_pending";
    public static final String DEFAULT_MAX_PENDING = "100000";

    public static final String CONFIG_KEY_CORES_FACTOR = "cores_factor";
    public static final String DEFAULT_CORES_FACTOR = "1.0";

    public static final String CONFIG_KEY_ADDITIONAL_THREADS = "additional_threads";
    public static final String DEFAULT_ADDITIONAL_THREADS = "1";

    public static final String CONFIG_KEY_HARD_SHUTDOWN = "hard_shutdown";
    public static final String DEFAULT_HARD_SHUTDOWN = "true";

    private ScheduledExecutorService schedule = null;
    private ExecutorService executor = null;
    private AtomicLong numLimited = null;
    private long maxNumWaitingLimitedTasks;
    private int threadPoolSize;

    private double m = Double.parseDouble(DEFAULT_CORES_FACTOR);
    private int additionalThreads = Integer.parseInt(DEFAULT_ADDITIONAL_THREADS);

    private final Supplier<String> nameSupplier;
    private boolean hardShutdown = Boolean.parseBoolean(DEFAULT_ADDITIONAL_THREADS);

    public DefaultThreadingModel(final Supplier<String> nameSupplier, final int threadPoolSize, final int maxNumWaitingLimitedTasks) {
        this.nameSupplier = nameSupplier;
        this.threadPoolSize = threadPoolSize;
        this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
    }

    public DefaultThreadingModel(final Supplier<String> nameSupplier) {
        this(nameSupplier, -1, Integer.parseInt(DEFAULT_MAX_PENDING));
    }

    public DefaultThreadingModel(final String threadNameBase) {
        this(new NameSupplier(threadNameBase));
    }

    /**
     * Create a DefaultDempsyExecutor with a fixed number of threads while setting the maximum number of limited tasks.
     */
    public DefaultThreadingModel(final String threadNameBase, final int threadPoolSize, final int maxNumWaitingLimitedTasks) {
        this(new NameSupplier(threadNameBase), threadPoolSize, maxNumWaitingLimitedTasks);
    }

    /**
     * <p>
     * Prior to calling start you can set the cores factor and additional cores. Ultimately the number of threads in the pool will be given by:
     * </p>
     * 
     * <p>
     * num threads = m * num cores + b
     * </p>
     * 
     * <p>
     * Where 'm' is set by setCoresFactor and 'b' is set by setAdditionalThreads
     * </p>
     */
    public DefaultThreadingModel setCoresFactor(final double m) {
        this.m = m;
        return this;
    }

    /**
     * <p>
     * Prior to calling start you can set the cores factor and additional cores. Ultimately the number of threads in the pool will be given by:
     * </p>
     * 
     * <p>
     * num threads = m * num cores + b
     * </p>
     * 
     * <p>
     * Where 'm' is set by setCoresFactor and 'b' is set by setAdditionalThreads
     * </p>
     */
    public DefaultThreadingModel setAdditionalThreads(final int additionalThreads) {
        this.additionalThreads = additionalThreads;
        return this;
    }

    /**
     * When closing this ThreadingModel, use a hard shutdown (shutdownNow) on the executors.
     */
    public DefaultThreadingModel setHardShutdown(final boolean hardShutdown) {
        this.hardShutdown = hardShutdown;
        return this;
    }

    @Override
    public DefaultThreadingModel start() {
        if (threadPoolSize == -1) {
            // figure out the number of cores.
            final int cores = Runtime.getRuntime().availableProcessors();
            final int cpuBasedThreadCount = (int) Math.ceil(cores * m) + additionalThreads; // why? I don't know. If you don't like it
                                                                                            // then use the other constructor
            threadPoolSize = Math.max(cpuBasedThreadCount, minNumThreads);
        }
        executor = Executors.newFixedThreadPool(threadPoolSize,
                r -> new Thread(r, nameSupplier.get()));
        schedule = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, nameSupplier.get() + "-Scheduled"));
        numLimited = new AtomicLong(0);

        return this;
    }

    private static String getConfigValue(final Map<String, String> conf, final String key, final String defaultValue) {
        final String entireKey = DefaultThreadingModel.class.getPackage().getName() + "." + key;
        return conf.containsKey(entireKey) ? conf.get(entireKey) : defaultValue;
    }

    public DefaultThreadingModel configure(final Map<String, String> configuration) {
        setMaxNumberOfQueuedLimitedTasks(Integer.parseInt(getConfigValue(configuration, CONFIG_KEY_MAX_PENDING, DEFAULT_MAX_PENDING)));
        setHardShutdown(Boolean.parseBoolean(getConfigValue(configuration, CONFIG_KEY_HARD_SHUTDOWN, DEFAULT_HARD_SHUTDOWN)));
        setCoresFactor(Double.parseDouble(getConfigValue(configuration, CONFIG_KEY_CORES_FACTOR, DEFAULT_CORES_FACTOR)));
        setAdditionalThreads(Integer.parseInt(getConfigValue(configuration, CONFIG_KEY_ADDITIONAL_THREADS, DEFAULT_ADDITIONAL_THREADS)));
        return this;
    }

    public int getMaxNumberOfQueuedLimitedTasks() {
        return (int) maxNumWaitingLimitedTasks;
    }

    public DefaultThreadingModel setMaxNumberOfQueuedLimitedTasks(final long maxNumWaitingLimitedTasks) {
        this.maxNumWaitingLimitedTasks = maxNumWaitingLimitedTasks;
        return this;
    }

    @Override
    public int getNumThreads() {
        return threadPoolSize;
    }

    @Override
    public void close() {
        if (hardShutdown) {
            if (executor != null)
                executor.shutdownNow();

            if (schedule != null)
                schedule.shutdownNow();
        } else {
            if (executor != null)
                executor.shutdown();

            if (schedule != null)
                schedule.shutdown();
        }
    }

    @Override
    public int getNumberLimitedPending() {
        return numLimited.intValue();
    }

    public boolean isRunning() {
        return (schedule != null && executor != null) &&
                !(schedule.isShutdown() || schedule.isTerminated()) &&
                !(executor.isShutdown() || executor.isTerminated());
    }

    @Override
    public <V> Future<V> submit(final Callable<V> r) {
        return executor.submit(r);
    }

    @Override
    public <V> Future<V> submitLimited(final Rejectable<V> r, final boolean count) {

        if (maxNumWaitingLimitedTasks > 0) { // maxNumWaitingLimitedTasks <= 0 means unlimited
            final Callable<V> task = () -> {
                if (!count)
                    return r.call();

                final long num = numLimited.decrementAndGet();
                if (num <= maxNumWaitingLimitedTasks)
                    return r.call();
                r.rejected();
                return null;
            };

            if (count)
                numLimited.incrementAndGet();

            final Future<V> ret = executor.submit(task);
            return ret;
        } else
            return executor.submit(() -> r.call());

    }

    @Override
    public <V> Future<V> schedule(final Callable<V> r, final long delay, final TimeUnit unit) {
        final ProxyFuture<V> ret = new ProxyFuture<V>();

        // here we are going to wrap the Callable and the Future to change the
        // submission to one of the other queues.
        ret.schedFuture = schedule.schedule(new Runnable() {
            Callable<V> callable = r;
            ProxyFuture<V> rret = ret;

            @Override
            public void run() {
                // now resubmit the callable we're proxying
                rret.set(submit(callable));
            }
        }, delay, unit);

        // proxy the return future.
        return ret;
    }

    private static class ProxyFuture<V> implements Future<V> {
        private volatile Future<V> ret;
        private volatile ScheduledFuture<?> schedFuture;

        private synchronized void set(final Future<V> f) {
            ret = f;
            if (schedFuture.isCancelled())
                ret.cancel(true);
            this.notifyAll();
        }

        // called only from synchronized methods
        private Future<?> getCurrent() {
            return ret == null ? schedFuture : ret;
        }

        @Override
        public synchronized boolean cancel(final boolean mayInterruptIfRunning) {
            return getCurrent().cancel(mayInterruptIfRunning);
        }

        @Override
        public synchronized boolean isCancelled() {
            return getCurrent().isCancelled();
        }

        @Override
        public synchronized boolean isDone() {
            return ret == null ? false : ret.isDone();
        }

        @Override
        public synchronized V get() throws InterruptedException, ExecutionException {
            while (ret == null)
                this.wait();
            return ret.get();
        }

        @Override
        public V get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            final long cur = System.currentTimeMillis();
            while (ret == null)
                this.wait(unit.toMillis(timeout));
            return ret.get(System.currentTimeMillis() - cur, TimeUnit.MILLISECONDS);
        }
    }

    public static class NameSupplier implements Supplier<String> {
        private final static AtomicLong threadNum = new AtomicLong();
        private final String threadNameBase;

        public NameSupplier(final String threadBaseName) {
            this.threadNameBase = threadBaseName;
        }

        @Override
        public String get() {
            return threadNameBase + threadNum.getAndIncrement();
        }
    }
}
