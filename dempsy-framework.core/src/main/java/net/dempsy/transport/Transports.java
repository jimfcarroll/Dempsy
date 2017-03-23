package net.dempsy.transport;

import java.util.Arrays;
import java.util.List;

public class Transports {

    public final Transport[] transports;

    public Transports(final Transport... transports) {
        this.transports = Arrays.copyOf(transports, transports.length);
    }

    public Transports(final List<Transport> transports) {
        this.transports = transports.toArray(new Transport[transports.size()]);
    }

}
