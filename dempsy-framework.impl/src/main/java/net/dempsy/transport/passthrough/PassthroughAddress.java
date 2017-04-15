package net.dempsy.transport.passthrough;

import java.util.concurrent.atomic.AtomicLong;

import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Sender;

public class PassthroughAddress implements NodeAddress {
    private static final long serialVersionUID = 1L;

    private final static AtomicLong destinationIdSequence = new AtomicLong(0);

    private final Sender sender;
    private final long destinationId;

    PassthroughAddress(final Sender sender) {
        this.sender = sender;
        destinationId = destinationIdSequence.getAndIncrement();
    }

    @Override
    public String toString() {
        return "Passthrough(" + destinationId + ")";
    }
}
