package net.dempsy;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import net.dempsy.config.ClusterId;
import net.dempsy.transport.NodeAddress;

public class NodeInformation implements Serializable {
    private static final long serialVersionUID = 1L;
    public final String transportTypeId;
    public final NodeAddress nodeAddress;
    public final Map<ClusterId, Set<String>> messageTypesByClusterId;

    public NodeInformation(final String transportTypeId, final NodeAddress nodeAddress,
            final Map<ClusterId, Set<String>> messageTypesByClusterId) {
        this.transportTypeId = transportTypeId;
        this.nodeAddress = nodeAddress;
        this.messageTypesByClusterId = Collections.unmodifiableMap(messageTypesByClusterId);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((messageTypesByClusterId == null) ? 0 : messageTypesByClusterId.hashCode());
        result = prime * result + ((nodeAddress == null) ? 0 : nodeAddress.hashCode());
        result = prime * result + ((transportTypeId == null) ? 0 : transportTypeId.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final NodeInformation other = (NodeInformation) obj;
        if (messageTypesByClusterId == null) {
            if (other.messageTypesByClusterId != null)
                return false;
        } else if (!messageTypesByClusterId.equals(other.messageTypesByClusterId))
            return false;
        if (nodeAddress == null) {
            if (other.nodeAddress != null)
                return false;
        } else if (!nodeAddress.equals(other.nodeAddress))
            return false;
        if (transportTypeId == null) {
            if (other.transportTypeId != null)
                return false;
        } else if (!transportTypeId.equals(other.transportTypeId))
            return false;
        return true;
    }

}