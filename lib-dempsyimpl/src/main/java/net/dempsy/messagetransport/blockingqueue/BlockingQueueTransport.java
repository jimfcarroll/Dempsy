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

package net.dempsy.messagetransport.blockingqueue;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.dempsy.executor.DempsyExecutor;
import net.dempsy.messagetransport.MessageTransportException;
import net.dempsy.messagetransport.OverflowHandler;
import net.dempsy.messagetransport.Receiver;
import net.dempsy.messagetransport.SenderFactory;
import net.dempsy.messagetransport.Transport;
import net.dempsy.monitoring.StatsCollector;

public class BlockingQueueTransport implements Transport {
    interface BlockingQueueFactory {
        public BlockingQueue<byte[]> createBlockingQueue();
    }

    private final BlockingQueueFactory queueSource = null;
    private OverflowHandler overflowHandler = null;

    @Override
    public SenderFactory createOutbound(final DempsyExecutor executor, final StatsCollector statsCollector) {
        final BlockingQueueSenderFactory ret = new BlockingQueueSenderFactory(statsCollector);
        if (overflowHandler != null)
            ret.setOverflowHandler(overflowHandler);
        return ret;
    }

    @Override
    public Receiver createInbound(final DempsyExecutor executor) throws MessageTransportException {
        final BlockingQueueAdaptor ret = new BlockingQueueAdaptor();
        ret.setQueue(getNewQueue());
        if (overflowHandler != null)
            ret.setOverflowHandler(overflowHandler);
        ret.start(); // @PostConstruct
        return ret;
    }

    @Override
    public void setOverflowHandler(final OverflowHandler overflowHandler) {
        this.overflowHandler = overflowHandler;
    }

    private BlockingQueue<byte[]> getNewQueue() {
        return (queueSource == null) ? new LinkedBlockingQueue<byte[]>() : queueSource.createBlockingQueue();
    }
}
