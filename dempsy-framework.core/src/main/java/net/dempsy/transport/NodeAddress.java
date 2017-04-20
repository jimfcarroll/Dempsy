package net.dempsy.transport;

import java.io.Serializable;

/**
 * This class represents and opaque handle to message transport implementation specific means of connecting to a destination.
 */
public interface NodeAddress extends Serializable {
    public default String getGuid() {
        return this.toString();
    }

}
