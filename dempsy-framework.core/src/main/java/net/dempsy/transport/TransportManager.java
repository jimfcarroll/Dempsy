package net.dempsy.transport;

import net.dempsy.Manager;

public class TransportManager extends Manager<SenderFactory> {
    public TransportManager() {
        super(SenderFactory.class);
    }
}
