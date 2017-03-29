package net.dempsy.container;

/**
 * This interface is to allow the getting of metrics within the tests.
 */
public interface MetricGetters {
    long getProcessedMessageCount();

    long getDispatchedMessageCount();

    long getMessageFailedCount();

    long getDiscardedMessageCount();

    long getMessageCollisionCount();

    int getInFlightMessageCount();

    long getMessagesNotSentCount();

    long getMessagesSentCount();

    long getMessagesReceivedCount();

    long getMessagesDispatched();

    double getPreInstantiationDuration();

    double getOutputInvokeDuration();

    double getEvictionDuration();

    long getMessageProcessorsCreated();

    long getMessageProcessorCount();

    long getMessageBytesSent();

    long getMessageBytesReceived();

    long getMessagesPending();

    long getMessagesOutPending();
}
