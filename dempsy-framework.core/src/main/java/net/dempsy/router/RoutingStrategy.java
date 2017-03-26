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

import net.dempsy.Service;
import net.dempsy.messages.KeyedMessageWithType;
import net.dempsy.transport.NodeAddress;

public interface RoutingStrategy {

    public static class ContainerAddress {
        public final NodeAddress node;
        public final int cluster;

        public ContainerAddress(final NodeAddress node, final int cluster) {
            this.node = node;
            this.cluster = cluster;
        }
    }

    public static interface Outbound extends Service {
        public ContainerAddress selectDestinationForMessage(KeyedMessageWithType message);
    }

    public static interface Inbound extends Service {
        public default String routingStrategyTypeId() {
            return this.getClass().getPackage().getName();
        }
    }
}
