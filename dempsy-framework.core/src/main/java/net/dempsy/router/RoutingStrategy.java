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

package net.dempsy.router;

import java.io.Serializable;
import java.util.Arrays;

import net.dempsy.Service;
import net.dempsy.config.ClusterId;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.transport.NodeAddress;

public interface RoutingStrategy {

    public static final class ContainerAddress implements Serializable {
        private static final long serialVersionUID = 1L;
        public final NodeAddress node;
        public final int[] clusters;

        @SuppressWarnings("unused")
        private ContainerAddress() {
            node = null;
            clusters = null;
        }

        public ContainerAddress(final NodeAddress node, final int cluster) {
            this.node = node;
            this.clusters = new int[] { cluster };
        }

        public ContainerAddress(final NodeAddress node, final int[] clusters) {
            this.node = node;
            this.clusters = clusters;
        }

        @Override
        public String toString() {
            return "ContainerAddress[node=" + node + ", clusters=" + Arrays.toString(clusters) + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(clusters);
            result = prime * result + ((node == null) ? 0 : node.hashCode());
            return result;
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (ContainerAddress.class != obj.getClass()) // can do this for a final class
                return false;
            final ContainerAddress other = (ContainerAddress) obj;
            if (!Arrays.equals(clusters, other.clusters))
                return false;
            if (node == null) {
                if (other.node != null)
                    return false;
            } else if (!node.equals(other.node))
                return false;
            return true;
        }
    }

    public static interface Factory extends Service {
        /**
         * Get the routing strategy associated with the downstream cluster denoted
         * but {@code clusterId}. There should be no need to {@code start()} the 
         * Router that's returned. Since the {@link Factory} is the manager for
         * the {@link Router}s the caller should {@code release} the {@link Router}
         * when it's done. 
         */
        public Router getStrategy(ClusterId clusterId);
    }

    public static interface Router {

        public ContainerAddress selectDestinationForMessage(KeyedMessageWithType message);

        /**
         * This will call release on the {@link Factory} that created it.
         */
        public void release();
    }

    public static interface Inbound extends Service {
        public void setContainerDetails(ClusterId clusterId, ContainerAddress address);

        /**
         * Since the {@link Inbound} has the responsibility to determine which instances of a 
         * MessageProcessors are valid in 'this' node, it should be able to provide that
         * information through the implementation of this method. 
         */
        public boolean doesMessageKeyBelongToNode(Object messageKey);

        public default String routingStrategyTypeId() {
            return this.getClass().getPackage().getName();
        }
    }
}
