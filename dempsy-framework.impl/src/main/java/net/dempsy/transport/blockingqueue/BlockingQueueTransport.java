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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.dempsy.monitoring.StatsCollector;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.Receiver;
import net.dempsy.transport.SenderFactory;
import net.dempsy.transport.Transport;

public class BlockingQueueTransport implements Transport {

    private final StatsCollector statsCollector;
    private final int capacity;
    private final boolean blocking;

    public BlockingQueueTransport(final int capacity, final boolean blocking, final StatsCollector statsCollector) {
        this.statsCollector = statsCollector;
        this.capacity = capacity;
        this.blocking = blocking;
    }

    public BlockingQueueTransport() {
        this(Integer.MAX_VALUE, true, null);
    }

    @Override
    public SenderFactory createOutbound() {
        return new BlockingQueueSenderFactory(blocking, statsCollector);
    }

    @Override
    public Receiver createInbound() throws MessageTransportException {
        final BlockingQueueReceiver ret = new BlockingQueueReceiver(getNewQueue());
        return ret;
    }

    private BlockingQueue<byte[]> getNewQueue() {
        return new LinkedBlockingQueue<byte[]>(capacity);
    }
}
