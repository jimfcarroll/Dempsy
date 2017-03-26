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

package net.dempsy.monitoring.basic;

import java.util.concurrent.atomic.AtomicLong;

import net.dempsy.container.MetricGetters;
import net.dempsy.monitoring.StatsCollector;

/**
 * A very basic implementation of StatsCollector.
 * Doesn't do all the fancy stuff the default Coda 
 * implementation does, but useful for testing.
 *
 */
public class BasicStatsCollector implements StatsCollector, MetricGetters {
    private final AtomicLong messagesReceived = new AtomicLong();
    private final AtomicLong messagesDiscarded = new AtomicLong();
    private final AtomicLong messagesCollisions = new AtomicLong();
    private final AtomicLong messagesDispatched = new AtomicLong();
    private final AtomicLong messagesProcessed = new AtomicLong();
    private final AtomicLong messagesFailed = new AtomicLong();
    private final AtomicLong messagesSent = new AtomicLong();
    private final AtomicLong messagesUnsent = new AtomicLong();
    private final AtomicLong inProcessMessages = new AtomicLong();
    private final AtomicLong numberOfMPs = new AtomicLong();
    private final AtomicLong mpsCreated = new AtomicLong();
    private final AtomicLong mpsDeleted = new AtomicLong();

    private final AtomicLong preInstantiationDuration = new AtomicLong();
    private final AtomicLong mpHandleMessageDuration = new AtomicLong();
    private final AtomicLong outputInvokeDuration = new AtomicLong();
    private final AtomicLong evictionPassDuration = new AtomicLong();

    private final AtomicLong bytesReceived = new AtomicLong();
    private final AtomicLong bytesSent = new AtomicLong();

    private Gauge currentMessagesPendingGauge = null;
    private Gauge currentMessagesOutPendingGauge = null;

    private static class BasicTimerContext implements StatsCollector.TimerContext {
        private final long startTime = System.currentTimeMillis();
        private final AtomicLong toIncrement;

        private BasicTimerContext(final AtomicLong toIncrement) {
            this.toIncrement = toIncrement;
        }

        @Override
        public void stop() {
            toIncrement.addAndGet(System.currentTimeMillis() - startTime);
        }
    }

    @Override
    public long getDiscardedMessageCount() {
        return messagesDiscarded.longValue();
    }

    @Override
    public long getMessageCollisionCount() {
        return messagesCollisions.longValue();
    }

    @Override
    public long getDispatchedMessageCount() {
        return messagesDispatched.longValue();
    }

    @Override
    public int getInFlightMessageCount() {
        return inProcessMessages.intValue();
    }

    @Override
    public long getMessageFailedCount() {
        return messagesFailed.longValue();
    }

    @Override
    public long getProcessedMessageCount() {
        return messagesProcessed.longValue();
    }

    @Override
    public void messageDiscarded(final Object message) {
        messagesDiscarded.incrementAndGet();
        inProcessMessages.decrementAndGet();
    }

    @Override
    public void messageCollision(final Object message) {
        messagesCollisions.incrementAndGet();
    }

    @Override
    public void messageDispatched(final Object message) {
        messagesDispatched.incrementAndGet();
        inProcessMessages.decrementAndGet();
    }

    @Override
    public void messageFailed(final boolean mpFailure) {
        messagesFailed.incrementAndGet();
        inProcessMessages.decrementAndGet();
    }

    @Override
    public void messageNotSent() {
        messagesUnsent.incrementAndGet();
    }

    @Override
    public void messageProcessed(final Object message) {
        messagesProcessed.incrementAndGet();
        inProcessMessages.decrementAndGet();
    }

    @Override
    public void messageProcessorCreated(final Object key) {
        mpsCreated.incrementAndGet();
        numberOfMPs.incrementAndGet();
    }

    @Override
    public void messageProcessorDeleted(final Object key) {
        mpsDeleted.incrementAndGet();
        numberOfMPs.decrementAndGet();
    }

    @Override
    public void messageReceived(final Object message) {
        messagesReceived.incrementAndGet();
    }

    @Override
    public void messageSent(final Object message) {
        messagesSent.incrementAndGet();
    }

    @Override
    public void stop() {
        // no-op

    }

    @Override
    public double getPreInstantiationDuration() {
        return preInstantiationDuration.doubleValue();
    }

    @Override
    public StatsCollector.TimerContext preInstantiationStarted() {
        return new BasicTimerContext(preInstantiationDuration);
    }

    @Override
    public StatsCollector.TimerContext handleMessageStarted() {
        return new BasicTimerContext(mpHandleMessageDuration);
    }

    @Override
    public double getOutputInvokeDuration() {
        return outputInvokeDuration.doubleValue();
    }

    @Override
    public StatsCollector.TimerContext outputInvokeStarted() {
        return new BasicTimerContext(outputInvokeDuration);
    }

    @Override
    public StatsCollector.TimerContext evictionPassStarted() {
        return new BasicTimerContext(evictionPassDuration);
    }

    @Override
    public double getEvictionDuration() {
        return evictionPassDuration.doubleValue();
    }

    @Override
    public long getMessagesNotSentCount() {
        return messagesUnsent.get();
    }

    @Override
    public long getMessagesSentCount() {
        return messagesSent.get();
    }

    @Override
    public long getMessagesReceivedCount() {
        return messagesReceived.get();
    }

    @Override
    public long getMessageProcessorsCreated() {
        return mpsCreated.get();
    }

    @Override
    public long getMessageProcessorCount() {
        return numberOfMPs.get();
    }

    @Override
    public long getMessageBytesSent() {
        return bytesSent.get();
    }

    @Override
    public long getMessageBytesReceived() {
        return bytesReceived.get();
    }

    @Override
    public synchronized void setMessagesPendingGauge(final Gauge currentMessagesPendingGauge) {
        this.currentMessagesPendingGauge = currentMessagesPendingGauge;
    }

    @Override
    public synchronized void setMessagesOutPendingGauge(final Gauge currentMessagesOutPendingGauge) {
        this.currentMessagesOutPendingGauge = currentMessagesOutPendingGauge;
    }

    @Override
    public synchronized long getMessagesPending() {
        return currentMessagesPendingGauge == null ? 0 : currentMessagesPendingGauge.value();
    }

    @Override
    public synchronized long getMessagesOutPending() {
        return currentMessagesOutPendingGauge == null ? 0 : currentMessagesOutPendingGauge.value();
    }

}
