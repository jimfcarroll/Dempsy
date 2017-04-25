package net.dempsy.monitoring.dummy;

import net.dempsy.monitoring.ClusterStatsCollector;

public class DummyClusterStatsCollector implements ClusterStatsCollector {

    @Override
    public void messageDispatched(final Object message) {}

    @Override
    public void messageProcessed(final Object message) {}

    @Override
    public void messageFailed(final boolean mpFailure) {}

    @Override
    public void messageCollision(final Object message) {}

    @Override
    public void messageProcessorCreated(final Object key) {}

    @Override
    public void messageProcessorDeleted(final Object key) {}

    @Override
    public void stop() {}

    @Override
    public TimerContext preInstantiationStarted() {
        return () -> {};
    }

    @Override
    public TimerContext outputInvokeStarted() {
        return () -> {};
    }

    @Override
    public TimerContext evictionPassStarted() {
        return () -> {};
    }

}
