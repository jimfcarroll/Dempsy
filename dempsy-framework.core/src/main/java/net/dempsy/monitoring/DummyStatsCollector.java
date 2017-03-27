package net.dempsy.monitoring;

public class DummyStatsCollector implements StatsCollector {

    @Override
    public void messageReceived(final Object message) {}

    @Override
    public void messageDispatched(final Object message) {}

    @Override
    public void messageProcessed(final Object message) {}

    @Override
    public void messageFailed(final boolean mpFailure) {}

    @Override
    public void messageSent(final Object message) {}

    @Override
    public void messageNotSent() {}

    @Override
    public void messageDiscarded(final Object message) {}

    @Override
    public void messageCollision(final Object message) {}

    @Override
    public void setMessagesPendingGauge(final Gauge currentMessagesPendingGauge) {}

    @Override
    public void setMessagesOutPendingGauge(final Gauge currentMessagesOutPendingGauge) {}

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
    public TimerContext handleMessageStarted() {
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
