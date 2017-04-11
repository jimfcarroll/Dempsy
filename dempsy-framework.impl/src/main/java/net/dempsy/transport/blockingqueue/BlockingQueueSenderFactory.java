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

package net.dempsy.transport.blockingqueue;

import java.util.HashMap;
import java.util.Map;

import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.Sender;
import net.dempsy.transport.SenderFactory;

public class BlockingQueueSenderFactory implements SenderFactory {
    private final Map<NodeAddress, BlockingQueueSender> senders = new HashMap<NodeAddress, BlockingQueueSender>();
    private NodeStatsCollector statsCollector;
    private boolean blocking = true;

    @Override
    public synchronized Sender getSender(final NodeAddress destination) throws MessageTransportException {
        BlockingQueueSender blockingQueueSender = senders.get(destination);
        if (blockingQueueSender == null) {
            blockingQueueSender = new BlockingQueueSender(((BlockingQueueAddress) destination).getQueue(), blocking, statsCollector);
            senders.put(destination, blockingQueueSender);
        }

        return blockingQueueSender;
    }

    @Override
    public synchronized void close() {
        for (final BlockingQueueSender sender : senders.values())
            sender.close();
    }

    @Override
    public void setStatsCollector(final NodeStatsCollector statsCollector) {
        this.statsCollector = statsCollector;
    }

    public BlockingQueueSenderFactory setBlocking(final boolean blocking) {
        this.blocking = blocking;
        return this;
    }

    @Override
    public boolean isReady() {
        // we're always ready
        return true;
    }
}
