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

import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import net.dempsy.threading.DefaultThreadingModel;
import net.dempsy.threading.ThreadingModel;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.Receiver;
import net.dempsy.transport.Sender;
import net.dempsy.transport.SenderFactory;
import net.dempsy.transport.Transport;
import net.dempsy.transport.blockingqueue.BlockingQueueTransport;

public class BlockingQueueTest {

    /*
     * Test basic functionality for the BlockingQueue implementation of Message Transport. Verify that messages sent to the Sender arrive at the receiver via handleMessage.
     */
    @Test
    public void testBlockingQueue() throws Exception {
        final Transport tp = new BlockingQueueTransport();
        final AtomicReference<String> message = new AtomicReference<String>(null);
        try (ThreadingModel tm = new DefaultThreadingModel();
                Receiver r = tp.createInbound();
                SenderFactory sf = tp.createOutbound();
                Sender sender = sf.getSender(r.getAddress())) {
            r.start(msg -> {
                message.set(new String(msg));
                return true;
            }, tm);
            sender.send("Hello".getBytes());
            assertTrue(poll(o -> "Hello".equals(message.get())));
        }
    }

    /**
     * Test a non-blocking Transport without an overflow handler on the transport. Should call overflow handler once queue is full.
     * 
     * @throws Throwable
     */
    @Test(expected = MessageTransportException.class)
    public void testNonBlockingQueueOverflow() throws Exception {
        final Transport tp = new BlockingQueueTransport(1, false, null);
        try (ThreadingModel tm = new DefaultThreadingModel();
                Receiver r = tp.createInbound();
                SenderFactory sf = tp.createOutbound();
                Sender sender = sf.getSender(r.getAddress())) {

            // Not starting the receiver.
            sender.send("Hello".getBytes()); // this should work
            sender.send("Hello again".getBytes()); // this should fail
        }

    }

    /**
     * Test overflow for a blocking Transport around a queue with depth one. While the transport will not call the, and does not even have a , overflow handler, every message will call the overflow handler on
     * the receiver since the queue is always full.
     * 
     * @throws Throwable
     */
    @Test
    public void testBlockingQueueOverflow() throws Throwable {
        final Transport tp = new BlockingQueueTransport(1, true, null);
        final AtomicReference<String> message = new AtomicReference<String>(null);
        try (final ThreadingModel tm = new DefaultThreadingModel();
                final Receiver r = tp.createInbound();
                final SenderFactory sf = tp.createOutbound();
                final Sender sender = sf.getSender(r.getAddress())) {

            final AtomicBoolean finallySent = new AtomicBoolean(false);
            final AtomicLong receiveCount = new AtomicLong();

            sender.send("Hello".getBytes()); // fill up queue

            final Thread t = new Thread(() -> {
                try {
                    sender.send("Hello again".getBytes());
                } catch (final MessageTransportException e) {
                    throw new RuntimeException(e);
                }
                finallySent.set(true);
            });
            t.start();

            Thread.sleep(100);
            assertFalse(finallySent.get()); // the thread should be hung blocked on the send

            // Start the receiver to read
            r.start(msg -> {
                message.set(new String(msg));
                receiveCount.incrementAndGet();
                return true;
            }, tm);

            // 2 messages should have been read and the 2nd should be "Hello again"
            assertTrue(poll(o -> "Hello again".equals(message.get())));

            // The thread should shut down eventually
            assertTrue(poll(o -> !t.isAlive()));
        }
    }
}
