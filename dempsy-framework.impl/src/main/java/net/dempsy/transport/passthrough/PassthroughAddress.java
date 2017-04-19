package net.dempsy.transport.passthrough;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.Listener;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Sender;

public class PassthroughAddress implements NodeAddress {
    private static final long serialVersionUID = 1L;

    private final static AtomicLong destinationIdSequence = new AtomicLong(0);
    private final static Map<Long, Sender> senders = new HashMap<>();
    private final static Long DEFAULT_ID = new Long(-1);

    private transient Sender sender;
    private final Long destinationId;

    @SuppressWarnings("unused")
    private PassthroughAddress() {
        sender = null;
        destinationId = DEFAULT_ID;
    }

    PassthroughAddress(final Sender sender) {
        this.sender = sender;
        destinationId = destinationIdSequence.getAndIncrement();
        synchronized (senders) {
            senders.put(destinationId, sender);
        }
    }

    Sender getSender(final NodeStatsCollector sc) {
        if (sender != null)
            return sender;
        synchronized (senders) {
            sender = senders.get(destinationId);
            if (sc != null)
                ((PassthroughSender) sender).setStatsCollector(sc);
        }
        return sender;
    }

    void setListener(final Listener<Object> listener) {
        ((PassthroughSender) getSender(null)).setListener(listener);
    }

    @Override
    public String toString() {
        return "Passthrough(" + destinationId + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        final long destinationIdLv = destinationId.longValue();
        result = prime * result + (int) (destinationIdLv ^ (destinationIdLv >>> 32));
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final PassthroughAddress other = (PassthroughAddress) obj;
        if (destinationId.longValue() != other.destinationId.longValue())
            return false;
        return true;
    }
}
