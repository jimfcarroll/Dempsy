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

package net.dempsy.messagetransport.tcp;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.core.Histogram;

import net.dempsy.TestUtils;
import net.dempsy.config.ClusterId;
import net.dempsy.executor.DefaultDempsyExecutor;
import net.dempsy.messagetransport.Destination;
import net.dempsy.messagetransport.Listener;
import net.dempsy.messagetransport.MessageTransportException;
import net.dempsy.messagetransport.Sender;
import net.dempsy.messagetransport.SenderFactory;
import net.dempsy.monitoring.StatsCollector;
import net.dempsy.monitoring.basic.BasicStatsCollector;
import net.dempsy.monitoring.coda.MetricGetters;
import net.dempsy.monitoring.coda.StatsCollectorFactoryCoda;

public class TcpTransportTest {
    static Logger logger = LoggerFactory.getLogger(TcpTransportTest.class);
    private static final int numThreads = 4;

    private static final long baseTimeoutMillis = 20000;

    // 2 is the TcpTransport message overhead for any message whos size is <= Short.MAX_VALUE
    private static final int tcpTransportHeaderSize = 2;

    private static final String charSet = "ABCDEFGHIJKLMNOPabcdefghijklmnop0123456789";
    private static final long maxOutboundQueueSize = 1000000;

    // -------------------------------------------------------------------------------------
    // Support code for multirun tests.
    // -------------------------------------------------------------------------------------

    public static interface Checker {
        public void check(int port, boolean localhost, long batchOutgoingMessagesDelayMillis) throws Throwable;
    }

    private static int failFastCalls = 0;

    private static boolean getFailFast() {
        failFastCalls++;
        return (failFastCalls & 0x00000001) != 0;
    }

    private void runAllCombinations(final Checker check) throws Throwable {
        logger.error("checking " + check + " with ephemeral port and not using \"localhost.\"");
        check.check(-1, false, 500);
        logger.error("checking " + check + " with port 8765 and not using \"localhost.\"");
        check.check(8765, false, -1);
        logger.error("checking " + check + " with ephemeral port using \"localhost.\"");
        check.check(-1, true, -1);
        logger.error("checking " + check + " with port 8765 using \"localhost.\"");
        check.check(8765, true, -1);
    }

    /**
     * Just send a simple message and make sure it gets through.
     */
    @Test
    public void testTransportInstantiation() throws Throwable {
        final AtomicBoolean batchedAtLeastOnce = new AtomicBoolean(false);

        runAllCombinations(new Checker() {
            @Override
            public void check(final int port, final boolean localhost, final long batchOutgoingMessagesDelayMillis) throws Throwable {
                final StatsCollector statsCollector = new StatsCollectorFactoryCoda().createStatsCollector(new ClusterId("test", "test-cluster"),
                        new Destination() {});

                SenderFactory factory = null;
                TcpReceiver adaptor = null;
                try {

                    final boolean shouldBatch = batchOutgoingMessagesDelayMillis >= 0;

                    if (shouldBatch)
                        batchedAtLeastOnce.set(true);

                    final TcpTransport transport = new TcpTransport();
                    transport.setFailFast(getFailFast());

                    // by default batching isn't disabled.
                    assertFalse(transport.isBatchingDisabled());

                    if (!shouldBatch)
                        transport.setDisableBatching(true);

                    if (!shouldBatch)
                        assertTrue(transport.isBatchingDisabled());

                    assertEquals(!shouldBatch, transport.isBatchingDisabled());

                    // ===========================================
                    // setup the sender and receiver
                    adaptor = (TcpReceiver) transport.createInbound(null);
                    adaptor.setStatsCollector(statsCollector);
                    final StringListener receiver = new StringListener();
                    adaptor.setListener(receiver);

                    factory = transport.createOutbound(null, statsCollector);
                    if (port > 0)
                        adaptor.setPort(port);
                    if (localhost)
                        adaptor.setUseLocalhost(localhost);
                    // ===========================================

                    adaptor.start(); // start the adaptor
                    final Destination destination = adaptor.getDestination(); // get the destination

                    // send a message
                    final byte[] messageBytes = "Hello".getBytes();
                    final Sender sender = factory.getSender(destination);

                    assertEquals((shouldBatch ? TcpTransport.defaultBatchingDelayMillis : -1),
                            ((TcpSender) sender).getBatchOutgoingMessagesDelayMillis());

                    sender.send(messageBytes);
                    sender.send(messageBytes);

                    // wait for it to be received.
                    for (final long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis()
                            && receiver.numMessages.get() < 2;)
                        Thread.sleep(1);
                    assertEquals(2, receiver.numMessages.get());

                    // verify everything came over ok.
                    assertEquals(1, receiver.receivedStringMessages.size());
                    assertEquals("Hello", receiver.receivedStringMessages.iterator().next());

                    if (shouldBatch) {
                        // verify the histogram
                        final Histogram histogram = ((TcpSender) sender).getBatchingHistogram();
                        assertEquals(calcMean(2), histogram.mean(), 0.0000001);
                        assertEquals(1, histogram.count());
                    }
                } finally {
                    if (factory != null)
                        factory.stop();

                    if (adaptor != null)
                        adaptor.stop();
                }

            }

            @Override
            public String toString() {
                return "testTransportInstantiation";
            }
        });

        assertTrue(batchedAtLeastOnce.get());
    }

    // -------------------------------------------------------------------------------------
    // multirun tests.
    // -------------------------------------------------------------------------------------
    /**
     * Just send a simple message and make sure it gets through.
     */
    @Test
    public void transportSimpleMessage() throws Throwable {
        runAllCombinations(new Checker() {
            @Override
            public void check(final int port, final boolean localhost, final long batchOutgoingMessagesDelayMillis) throws Throwable {
                try (final StatsCollector statsCollector = new BasicStatsCollector();
                        final SenderFactory factory = makeSenderFactory(false, statsCollector, batchOutgoingMessagesDelayMillis); // distruptible sender factory
                        final TcpReceiver adaptor = new TcpReceiver(null, getFailFast());) {
                    // ===========================================
                    // setup the sender and receiver

                    adaptor.setStatsCollector(statsCollector);
                    final StringListener receiver = new StringListener();
                    adaptor.setListener(receiver);

                    if (port > 0)
                        adaptor.setPort(port);
                    if (localhost)
                        adaptor.setUseLocalhost(localhost);
                    // ===========================================

                    adaptor.start(); // start the adaptor
                    final Destination destination = adaptor.getDestination(); // get the destination

                    // send a message
                    final Sender sender = factory.getSender(destination);
                    sender.send("Hello".getBytes());

                    // wait for it to be received.
                    for (final long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis()
                            && receiver.numMessages.get() == 0;)
                        Thread.sleep(1);
                    assertEquals(1, receiver.numMessages.get());

                    // verify everything came over ok.
                    assertEquals(1, receiver.receivedStringMessages.size());
                    assertEquals("Hello", receiver.receivedStringMessages.iterator().next());
                    adaptor.stop();
                }
            }

            @Override
            public String toString() {
                return "testTransportSimpleMessage";
            }
        });
    }

    public static class BatchingOutputStreamWatcher implements OutputStreamFactory {
        final long[] flushByteCounts;

        public BatchingOutputStreamWatcher(final long[] flushByteCounts) {
            this.flushByteCounts = flushByteCounts;
        }

        @Override
        public OutputStream makeOutputStream(final OutputStream socketOutputStream, final Socket socket) throws IOException {
            return new OutputStream() {
                final OutputStream proxy = socketOutputStream;
                int curCount = 0;

                @Override
                public void write(final int b) throws IOException {
                    flushByteCounts[curCount]++;
                }

                @Override
                public void flush() throws IOException {
                    curCount++;
                    proxy.flush();
                }

                @Override
                public void close() throws IOException {
                    proxy.close();
                }

                @Override
                public void write(final byte b[], final int off, final int len) throws IOException {
                    flushByteCounts[curCount] += len;
                    proxy.write(b, off, len);
                }
            };
        }
    }

    private static final double calcMean(final long... values) {
        double ret = 0.0;
        for (final long val : values)
            ret += val;
        return ret / values.length;
    }

    /**
     * This test simply checks the batching bookeepeing in the case were the message flow times out.
     */
    @Test
    public void transportBatchingTimeout() throws Throwable {
        runAllCombinations(new Checker() {
            @Override
            public void check(final int port, final boolean localhost, long batchOutgoingMessagesDelayMillis) throws Throwable {

                // we're going to batch no matter what the parameter says.
                // We want the batching timeout to be really small
                batchOutgoingMessagesDelayMillis = 175;

                // we want to keep track of the number of bytes written
                final long[] flushByteCounts = new long[4]; //

                try (final StatsCollector statsCollector = new StatsCollectorFactoryCoda().createStatsCollector(
                        new ClusterId("test", "test-cluster"),
                        new Destination() {});
                        final SenderFactory factory = makeSenderFactory(new BatchingOutputStreamWatcher(flushByteCounts), statsCollector,
                                batchOutgoingMessagesDelayMillis);
                        final TcpReceiver adaptor = new TcpReceiver(null, getFailFast());) {
                    // ===========================================
                    // setup the sender and receiver
                    adaptor.setStatsCollector(statsCollector);
                    final StringListener receiver = new StringListener();
                    adaptor.setListener(receiver);

                    if (port > 0)
                        adaptor.setPort(port);
                    if (localhost)
                        adaptor.setUseLocalhost(localhost);
                    // ===========================================

                    adaptor.start(); // start the adaptor
                    final Destination destination = adaptor.getDestination(); // get the destination

                    // send a message
                    final Sender sender = factory.getSender(destination);

                    // send messageBytes
                    final byte[] messageBytes = "Hello".getBytes();
                    final int sentBytesPerMessage = messageBytes.length + tcpTransportHeaderSize;

                    sender.send(messageBytes);
                    Thread.sleep(batchOutgoingMessagesDelayMillis * 2);
                    sender.send(messageBytes);
                    sender.send(messageBytes);
                    Thread.sleep(batchOutgoingMessagesDelayMillis * 2);
                    sender.send(messageBytes);

                    // now numBytesLastFlush should be set to the num of bytes that were last flushed.
                    // numBytesSent should be the total bytes, even those still in the buffer.
                    // Therefore numBytesSent - numBytesLastFlush should be the number of bytes waiting.
                    // These are asserted below.

                    // we've now sent one message more than what's required to cause a flush.

                    // wait for it to be received.
                    for (final long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis()
                            && receiver.numMessages.get() < 4;)
                        Thread.sleep(1);
                    Thread.sleep(10);
                    assertEquals(4, receiver.numMessages.get());

                    // verify everything came over ok.
                    assertEquals(1, receiver.receivedStringMessages.size());
                    assertEquals("Hello", receiver.receivedStringMessages.iterator().next());

                    assertEquals(sentBytesPerMessage, flushByteCounts[0]);
                    assertEquals(sentBytesPerMessage * 2, flushByteCounts[1]);
                    assertEquals(sentBytesPerMessage, flushByteCounts[2]);
                    assertEquals(0, flushByteCounts[3]);

                    // verify the histogram
                    final Histogram histogram = ((TcpSender) sender).getBatchingHistogram();
                    assertEquals(calcMean(1, 2, 1), histogram.mean(), 0.0000001);
                    assertEquals(3, histogram.count());
                }
            }

            @Override
            public String toString() {
                return "transportBatchingTimeout";
            }
        });
    }

    /**
     * This test simply checks the batching bookkeeping in the simple case. It sets the batch wait time stupidly high in order to make sure that exactly one full batch cycle happens and makes sure that the
     * correct number of bytes were accumulated, flushed, and left over.
     */
    @Test
    public void transportBatching() throws Throwable {
        runAllCombinations(new Checker() {
            @Override
            public void check(final int port, final boolean localhost, long batchOutgoingMessagesDelayMillis) throws Throwable {

                // we're going to batch no matter what the parameter says.
                batchOutgoingMessagesDelayMillis = 10000;

                // we want to keep track of the number of bytes written
                final long[] flushByteCounts = new long[2]; // This should be all that's needed

                try (final StatsCollector statsCollector = new StatsCollectorFactoryCoda().createStatsCollector(new ClusterId("test", "test-cluster"),
                        new Destination() {});
                        SenderFactory factory = makeSenderFactory(new BatchingOutputStreamWatcher(flushByteCounts), statsCollector,
                                batchOutgoingMessagesDelayMillis);
                        TcpReceiver adaptor = new TcpReceiver(null, getFailFast());) {
                    // ===========================================
                    // setup the sender and receiver
                    adaptor.setStatsCollector(statsCollector);
                    final StringListener receiver = new StringListener();
                    adaptor.setListener(receiver);

                    if (port > 0)
                        adaptor.setPort(port);
                    if (localhost)
                        adaptor.setUseLocalhost(localhost);
                    // ===========================================

                    adaptor.start(); // start the adaptor
                    final Destination destination = adaptor.getDestination(); // get the destination

                    // send a message
                    final Sender sender = factory.getSender(destination);

                    final int mtu = ((TcpSender) sender).getMtu();

                    // send enough messages to get one through
                    final byte[] messageBytes = "Hello".getBytes();
                    int numMessagesSent = 0;
                    int numBytesSent = 0;
                    int numBytesLastFlush = 0;
                    while (numBytesSent < mtu) {
                        sender.send(messageBytes);
                        numBytesLastFlush = numBytesSent;
                        numBytesSent += messageBytes.length + tcpTransportHeaderSize;
                        numMessagesSent++;
                    }

                    // now numBytesLastFlush should be set to the num of bytes that were last flushed.
                    // numBytesSent should be the total bytes, even those still in the buffer.
                    // Therefore numBytesSent - numBytesLastFlush should be the number of bytes waiting.
                    // These are asserted below.

                    // we've now sent one message more than what's required to cause a flush.

                    // wait for it to be received.
                    for (final long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis()
                            && receiver.numMessages.get() < numMessagesSent;)
                        Thread.sleep(1);
                    Thread.sleep(10);
                    assertEquals(numMessagesSent, receiver.numMessages.get());

                    // verify everything came over ok.
                    assertEquals(1, receiver.receivedStringMessages.size());
                    assertEquals("Hello", receiver.receivedStringMessages.iterator().next());

                    assertEquals(numBytesLastFlush, flushByteCounts[0]);
                    assertEquals(numBytesSent - numBytesLastFlush, flushByteCounts[1]);

                    // verify the histogram
                    final Histogram histogram = ((TcpSender) sender).getBatchingHistogram();
                    assertEquals(calcMean((numMessagesSent - 1), 1), histogram.mean(), 0.0000001);
                    assertEquals(2, histogram.count());
                }
            }

            @Override
            public String toString() {
                return "transportBatching";
            }
        });
    }

    /**
     * This test simply checks the batching bookeepeing in the case where the message sent is larger than the Mtu
     */
    @Test
    public void transportBatchingMessageLargerThanMtu() throws Throwable {
        runAllCombinations(new Checker() {
            @Override
            public void check(final int port, final boolean localhost, long batchOutgoingMessagesDelayMillis) throws Throwable {

                // we're going to batch no matter what the parameter says.
                batchOutgoingMessagesDelayMillis = 10000;

                // we want to keep track of the number of bytes written
                final long[] flushByteCounts = new long[2]; // This should be all that's needed

                try (final StatsCollector statsCollector = new StatsCollectorFactoryCoda().createStatsCollector(new ClusterId("test", "test-cluster"),
                        new Destination() {});
                        final TcpReceiver adaptor = new TcpReceiver(null, getFailFast());
                        final TcpReceiver adaptor2 = new TcpReceiver(null, getFailFast());
                        final SenderFactory factory = makeSenderFactory(new BatchingOutputStreamWatcher(flushByteCounts), statsCollector,
                                batchOutgoingMessagesDelayMillis);) {
                    // ===========================================
                    // setup the sender and receiver
                    adaptor.setStatsCollector(statsCollector);
                    final StringListener receiver = new StringListener();
                    adaptor.setListener(receiver);

                    if (port > 0)
                        adaptor.setPort(port);
                    if (localhost)
                        adaptor.setUseLocalhost(localhost);
                    // ===========================================

                    adaptor.start(); // start the adaptor
                    final Destination destination = adaptor.getDestination(); // get the destination
                    final Destination destination2 = adaptor2.getDestination(); // get the destination

                    // send a message
                    final Sender sender = factory.getSender(destination);
                    factory.getSender(destination2); // create a second sender to make sure the historgram acts correctly

                    final int mtu = ((TcpSender) sender).getMtu();

                    // now create a message larger than the mtu by 10%
                    final Random random = new Random();
                    final byte[] messageBytes = new byte[(int) (mtu * 1.1)];
                    for (int i = 0; i < messageBytes.length; i++)
                        messageBytes[i] = (byte) charSet.charAt(random.nextInt(charSet.length()));

                    // send the message ... it should be sent without delay.
                    sender.send(messageBytes);

                    // wait for it to be received.
                    for (final long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis()
                            && receiver.numMessages.get() == 0;)
                        Thread.sleep(1);
                    Thread.sleep(10);
                    assertEquals(1, receiver.numMessages.get());

                    // verify everything came over ok.
                    assertEquals(1, receiver.receivedStringMessages.size());

                    assertEquals(messageBytes.length + tcpTransportHeaderSize, flushByteCounts[0]);
                    assertEquals(0, flushByteCounts[1]);

                    // verify the histogram
                    final Histogram histogram = ((TcpSender) sender).getBatchingHistogram();
                    assertEquals(1.0D, histogram.mean(), 0.0000001);
                    assertEquals(1, histogram.count());
                }
            }

            @Override
            public String toString() {
                return "transportBatchingMessageLargerThanMtu";
            }
        });
    }

    /**
     * This test simply checks the batching bookeepeing in the case where the message sent is larger than the Mtu
     */
    @Test
    public void transportBatchingMessageLargerAndSmallerMixed() throws Throwable {
        runAllCombinations(new Checker() {
            @Override
            public void check(final int port, final boolean localhost, long batchOutgoingMessagesDelayMillis) throws Throwable {

                // we're going to batch no matter what the parameter says.
                batchOutgoingMessagesDelayMillis = 10000;

                // we want to keep track of the number of bytes written
                final long[] flushByteCounts = new long[4]; // This should be all that's needed

                try (final StatsCollector statsCollector = new StatsCollectorFactoryCoda().createStatsCollector(new ClusterId("test", "test-cluster"),
                        new Destination() {});
                        final SenderFactory factory = makeSenderFactory(new BatchingOutputStreamWatcher(flushByteCounts), statsCollector,
                                batchOutgoingMessagesDelayMillis);
                        final TcpReceiver adaptor = new TcpReceiver(null, getFailFast());) {
                    // ===========================================
                    // setup the sender and receiver
                    adaptor.setStatsCollector(statsCollector);
                    final StringListener receiver = new StringListener();
                    adaptor.setListener(receiver);

                    if (port > 0)
                        adaptor.setPort(port);
                    if (localhost)
                        adaptor.setUseLocalhost(localhost);
                    // ===========================================

                    adaptor.start(); // start the adaptor
                    final Destination destination = adaptor.getDestination(); // get the destination

                    // send a message
                    final Sender sender = factory.getSender(destination);

                    final int mtu = ((TcpSender) sender).getMtu();

                    // now create a message larger than the mtu by 10%
                    final Random random = new Random();
                    final byte[] largeMessageBytes = new byte[(int) (mtu * 1.1)];
                    for (int i = 0; i < largeMessageBytes.length; i++)
                        largeMessageBytes[i] = (byte) charSet.charAt(random.nextInt(charSet.length()));

                    final byte[] messageBytes = "Hello".getBytes();

                    // send the message ... it should be sent without delay.
                    sender.send(largeMessageBytes); // this should flush everything
                    sender.send(messageBytes); // this should wait for another message.
                    sender.send(messageBytes); // this should wait for another message.
                    sender.send(largeMessageBytes); // this should flush everything

                    // wait for it to be received.
                    for (final long endTime = System.currentTimeMillis() + baseTimeoutMillis; endTime > System.currentTimeMillis()
                            && receiver.numMessages.get() < 4;)
                        Thread.sleep(1);
                    Thread.sleep(10);
                    assertEquals(4, receiver.numMessages.get());

                    // verify everything came over ok.
                    assertEquals(2, receiver.receivedStringMessages.size());

                    assertEquals(largeMessageBytes.length + tcpTransportHeaderSize, flushByteCounts[0]);
                    assertEquals((messageBytes.length + tcpTransportHeaderSize) * 2, flushByteCounts[1]);
                    assertEquals(largeMessageBytes.length + tcpTransportHeaderSize, flushByteCounts[2]);
                    assertEquals(0, flushByteCounts[3]);

                    // verify the histogram
                    final Histogram histogram = ((TcpSender) sender).getBatchingHistogram();
                    assertEquals(calcMean(1, 2, 1), histogram.mean(), 0.0000001);
                    assertEquals(3, histogram.count());
                }
            }

            @Override
            public String toString() {
                return "transportBatchingMessageLargerAndSmallerMixed";
            }
        });
    }

    /**
     * This test reproduces a problem where the unsent count spikes wildly when a receiver disconnects.
     */
    @Test
    public void transportBatchingUnsentMessageCountDuringReceiverFailure() throws Throwable {
        runAllCombinations(new Checker() {
            @Override
            public void check(final int port, final boolean localhost, final long batchOutgoingMessagesDelayMillis) throws Throwable {
                // we want to run this always batching (since this is a batching test) but we want it
                // to work from both extremes.
                runCheck(port, localhost, 2000);
                runCheck(port, localhost, 0);
            }

            private void runCheck(final int port, final boolean localhost, final long batchOutgoingMessagesDelayMillis) throws Throwable {
                try (final StatsCollector statsCollector = new StatsCollectorFactoryCoda().createStatsCollector(
                        new ClusterId("test", "test-clustertransportBatchingUnsentMessageCountDuringReceiverFailure"), new Destination() {});
                        final TcpReceiver adaptor = new TcpReceiver(null, getFailFast());
                        // we want to keep track of the number of bytes written
                        final SenderFactory factory = makeSenderFactory((socketOutputStream, socket) -> new OutputStream() {
                            public int numFailures = 0;

                            @Override
                            public void write(final int b) {}

                            @Override
                            public void write(final byte b[], final int off, final int len) throws IOException {
                                numFailures++;
                                System.out.println("Num Failures:" + numFailures);
                                throw new IOException("Fake IOException");
                            }
                        }, statsCollector, batchOutgoingMessagesDelayMillis);) {
                    // ===========================================
                    // setup the sender and receiver
                    adaptor.setStatsCollector(statsCollector);
                    System.out.println("Starting number: " + (int) ((MetricGetters) statsCollector).getMessagesNotSentCount());

                    if (port > 0)
                        adaptor.setPort(port);
                    if (localhost)
                        adaptor.setUseLocalhost(localhost);
                    // ===========================================

                    adaptor.start(); // start the adaptor
                    final Destination destination = adaptor.getDestination(); // get the destination

                    // send a message
                    final TcpSender sender = (TcpSender) factory.getSender(destination);

                    final BlockingQueue<byte[]> internalQueue = sender.sendingQueue;

                    final int mtu = sender.getMtu();

                    // send enough messages to get one through
                    final byte[] messageBytes = "Hello".getBytes();
                    int numMessagesSent = 0;
                    int numBytesSent = 0;
                    while (true) {
                        final int nextBytesSent = numBytesSent + messageBytes.length + tcpTransportHeaderSize;
                        if (nextBytesSent > mtu)
                            break; // don't do the send if we're going to go over the mtu
                        sender.send(messageBytes);
                        numBytesSent += messageBytes.length + tcpTransportHeaderSize;
                        numMessagesSent++;
                    }

                    // give the sender thread time to get the queued messages into the output buffer.
                    assertTrue(TestUtils.poll(baseTimeoutMillis * 9999, internalQueue, o -> o.size() == 0));

                    adaptor.stop(); // rip out the downstream to avoid reconnects
                    Thread.sleep(200); // give it some time to stop.

                    // now make it do the send which should fail.
                    sender.send(messageBytes);
                    numMessagesSent++;

                    // now wait for a few (potential) failures.
                    assertTrue(TestUtils.poll(baseTimeoutMillis * 9999, numMessagesSent,
                            nms -> {
                                System.out.println(((MetricGetters) statsCollector).getMessagesNotSentCount());
                                return nms == (int) ((MetricGetters) statsCollector).getMessagesNotSentCount();
                            }));

                    // wait some time, then double check
                    Thread.sleep(200);

                    // now make sure there wheren't any changes that came in after
                    assertEquals(numMessagesSent, (int) ((MetricGetters) statsCollector).getMessagesNotSentCount());
                }
            }

            @Override
            public String toString() {
                return "transportBatchingUnsentMessageCountDuringReceiverFailure";
            }
        });
    }

    static TcpReceiver makeHangingReceiver() {
        return new TcpReceiver(null, getFailFast()) {
            int count = 0;

            @Override
            protected ClientThread makeNewClientThread(final Socket clientSocket) throws IOException {
                count++;
                return count == 1 ? new ClientThread(clientSocket) {
                    @Override
                    public void run() {
                        while (!stopClient.get())
                            try {
                                Thread.sleep(1);
                            } catch (final Throwable th) {}
                    }
                } : new ClientThread(clientSocket);
            }
        };
    }

    @Test
    public void transportSimpleMessageHungReciever() throws Throwable {
        runAllCombinations(new Checker() {
            @Override
            public void check(final int port, final boolean localhost, final long batchOutgoingMessagesDelayMillis) throws Throwable {
                SenderFactory factory = null;
                TcpReceiver adaptor = null;
                final byte[] message = new byte[1024 * 8];
                final BasicStatsCollector statsCollector = new BasicStatsCollector();
                try {
                    // ===========================================
                    // setup the sender and receiver
                    adaptor = makeHangingReceiver();

                    adaptor.setStatsCollector(statsCollector);
                    final StringListener receiver = new StringListener();
                    adaptor.setListener(receiver);
                    factory = makeSenderFactory(false, statsCollector, batchOutgoingMessagesDelayMillis);

                    if (port > 0)
                        adaptor.setPort(port);
                    if (localhost)
                        adaptor.setUseLocalhost(localhost);
                    // ===========================================

                    adaptor.start(); // start the adaptor
                    final Destination destination = adaptor.getDestination(); // get the destination

                    // send a message
                    final TcpSender sender = (TcpSender) factory.getSender(destination);
                    sender.setTimeoutMillis(100);

                    // wait for it to fail
                    assertTrue(TestUtils.poll(baseTimeoutMillis, statsCollector, new TestUtils.Condition<BasicStatsCollector>() {
                        @Override
                        public boolean conditionMet(final BasicStatsCollector o) throws Throwable {
                            // this should eventually fail
                            sender.send(message); // this should work
                            return o.getMessagesNotSentCount() > 0;
                        }
                    }));

                    Thread.sleep(100);

                    final Long numMessagesReceived = receiver.numMessages.get();
                    assertTrue(TestUtils.poll(baseTimeoutMillis, numMessagesReceived, new TestUtils.Condition<Long>() {
                        @Override
                        public boolean conditionMet(final Long o) throws Throwable {
                            // this should eventually fail
                            sender.send("Hello".getBytes()); // this should work
                            return receiver.numMessages.get() > o.longValue();
                        }
                    }));
                } finally {
                    if (factory != null)
                        factory.stop();

                    if (adaptor != null)
                        adaptor.stop();
                }

            }

            @Override
            public String toString() {
                return "transportSimpleMessageHungReciever";
            }
        });
    }

    // these are for the following test.
    private byte[] receivedByteArrayMessage;
    private CountDownLatch receiveLargeMessageLatch;

    /**
     * Test the sending and recieving of a large message (10 meg).
     * 
     * @throws Throwable
     */
    @Test
    public void transportLargeMessage() throws Throwable {
        final boolean localhost = false;

        try (final SenderFactory factory = makeSenderFactory(false, null, 500); // distruptible sender factory
                final TcpReceiver adaptor = new TcpReceiver(null, getFailFast());
                final TcpSenderFactory senderFactory = makeSenderFactory(false, null, 500);) {

            // ===========================================
            // setup the sender and receiver
            receiveLargeMessageLatch = new CountDownLatch(1);
            adaptor.setListener(new Listener() {
                @Override
                public boolean onMessage(final byte[] messageBytes, final boolean failfast) throws MessageTransportException {
                    receivedByteArrayMessage = messageBytes;
                    receiveLargeMessageLatch.countDown();
                    return true;
                }

                @Override
                public void shuttingDown() {}
            });

            adaptor.setUseEphemeralPort(true);
            if (localhost)
                adaptor.setUseLocalhost(localhost);
            // ===========================================

            adaptor.start(); // start the adaptor
            adaptor.start(); // double start ... just want more coverage.

            final Destination destination = adaptor.getDestination(); // get the destination
            adaptor.setPort(-1);
            if (localhost)
                adaptor.setUseLocalhost(localhost);

            final int size = 1024 * 1024 * 10;
            final byte[] tosend = new byte[size];
            for (int i = 0; i < size; i++)
                tosend[i] = (byte) i;

            final TcpSender sender = (TcpSender) senderFactory.getSender(destination);
            sender.setTimeoutMillis(100000); // extend the timeout because of the larger messages
            sender.send(tosend);

            assertTrue(receiveLargeMessageLatch.await(1, TimeUnit.MINUTES));

            assertArrayEquals(tosend, receivedByteArrayMessage);

        } finally {
            receivedByteArrayMessage = null;
        }
    }

    /**
     * This test checks the various behaviors of a tcp transport when the client is disrupted at the 5th byte. Multiple senders are started from multiple threads and ONLY ONE fails at the 5th byte. All senders
     * should be able to continue on when that happens and so the message counts should increase.
     */
    @Test
    public void transportMultipleConnectionsFailedClient() throws Throwable {
        runAllCombinations(new Checker() {
            @Override
            public void check(final int port, final boolean localhost, final long batchOutgoingMessagesDelayMillis) throws Throwable {
                try (final BasicStatsCollector statsCollector = new BasicStatsCollector();
                        final SenderFactory factory = makeSenderFactory(true, statsCollector, batchOutgoingMessagesDelayMillis); // distruptible sender factory
                        final TcpReceiver adaptor = new TcpReceiver(null, getFailFast());) {
                    // ===========================================
                    // setup the sender and receiver
                    adaptor.setStatsCollector(statsCollector);

                    if (port > 0)
                        adaptor.setPort(port);
                    if (localhost)
                        adaptor.setUseLocalhost(localhost);
                    // ===========================================

                    adaptor.start(); // start the adaptor
                    final Destination destination = adaptor.getDestination(); // get the destination

                    // ===========================================
                    // Start up sender threads and save the off
                    final ArrayList<Thread> threadsToJoinOn = new ArrayList<Thread>();
                    final SenderRunnable[] senders = new SenderRunnable[numThreads];
                    for (int i = 0; i < numThreads; i++)
                        threadsToJoinOn.add(i, new Thread(
                                senders[i] = new SenderRunnable(destination, i, factory), "Test Sender for " + i));

                    for (final Thread thread : threadsToJoinOn)
                        thread.start();
                    // ===========================================

                    // ===========================================
                    // check that one sender has failed since this is disruptable.
                    assertTrue(TestUtils.poll(baseTimeoutMillis, statsCollector, new TestUtils.Condition<BasicStatsCollector>() {
                        @Override
                        public boolean conditionMet(final BasicStatsCollector o) throws Throwable {
                            return o.getMessagesNotSentCount() > 0;
                        }
                    }));
                    // ===========================================

                    // ===========================================
                    // check that ONLY one failed (the others didn't) when we're not batching. Otherwise
                    // more may fail.
                    Thread.sleep(10);
                    if (batchOutgoingMessagesDelayMillis >= 0) // if we're batching then we only expect a failure but we don't know how many
                        assertTrue(statsCollector.getMessagesNotSentCount() > 0);
                    else
                        assertEquals(1, statsCollector.getMessagesNotSentCount());
                    // ===========================================

                    // all of the counts should increase.
                    final long[] curCounts = new long[numThreads];
                    int i = 0;
                    for (final SenderRunnable sender : senders)
                        curCounts[i++] = sender.sentMessageCount.get();

                    // ==========================================
                    // go until they are all higher. The Senders should still be running
                    // and sending successfully (only one failed once) so all counts should
                    // be increasing.
                    boolean allHigher = false;
                    for (final long endTime = System.currentTimeMillis() + numThreads * (baseTimeoutMillis); endTime > System.currentTimeMillis()
                            && !allHigher;) {
                        allHigher = true;
                        Thread.sleep(1);
                        i = 0;
                        for (final SenderRunnable sender : senders)
                            if (curCounts[i++] >= sender.sentMessageCount.get())
                                allHigher = false;
                    }

                    assertTrue(allHigher);
                    // ==========================================

                    // ==========================================
                    // Stop the senders.
                    for (final SenderRunnable sender : senders)
                        sender.keepGoing.set(false);

                    // wait until all threads are stopped
                    for (final Thread t : threadsToJoinOn)
                        t.join(5000);

                    // make sure everything actually stopped.
                    for (final SenderRunnable sender : senders)
                        assertTrue(sender.isStopped.get());
                    // ==========================================

                }
            }

            @Override
            public String toString() {
                return "transportMultipleConnectionsFailedClient";
            }
        });
    }

    /**
     * This test sends a few messages through (and checks that they were recived) and then the adaptor is stopped. This should cause all of the sender threads to exit since they are set to exit on a problem.
     * 
     * This also checks that if the listener throws an exception, the adaptor continues on it's merry way.
     */
    @Test
    public void transportMultipleConnectionsFailedReceiver() throws Throwable {
        runAllCombinations(new Checker() {
            @Override
            public void check(final int port, final boolean localhost, final long batchOutgoingMessagesDelayMillis) throws Throwable {
                try (final BasicStatsCollector statsCollector = new BasicStatsCollector();
                        final SenderFactory factory = makeSenderFactory(false, statsCollector, batchOutgoingMessagesDelayMillis); // not disruptible
                        final TcpReceiver adaptor = new TcpReceiver(null, getFailFast());) {
                    // ===========================================
                    // setup the sender and receiver
                    adaptor.setStatsCollector(statsCollector);
                    final StringListener receiver = new StringListener();
                    adaptor.setListener(receiver);

                    if (port > 0)
                        adaptor.setPort(port);
                    if (localhost)
                        adaptor.setUseLocalhost(localhost);
                    // ===========================================

                    adaptor.start(); // start the adaptor
                    final Destination destination = adaptor.getDestination(); // get the destination

                    // ===========================================
                    // Start up sender threads and save the off
                    final ArrayList<Thread> threadsToJoinOn = new ArrayList<Thread>();
                    final SenderRunnable[] senders = new SenderRunnable[numThreads];
                    for (int i = 0; i < numThreads; i++)
                        threadsToJoinOn.add(i, new Thread(
                                senders[i] = new SenderRunnable(destination, i, factory).stopOnNormalFailure(true),
                                "Test Sender for " + i));

                    for (final Thread thread : threadsToJoinOn)
                        thread.start();
                    // ===========================================

                    // ===========================================
                    // Wait until they all send a messgage or two
                    for (final SenderRunnable sender : senders)
                        for (final long endTime = System.currentTimeMillis() + numThreads * baseTimeoutMillis; endTime > System.currentTimeMillis()
                                && sender.sentMessageCount.get() == 0;)
                            Thread.sleep(1);

                    // assert that they all sent a message
                    for (final SenderRunnable sender : senders)
                        assertTrue(sender.sentMessageCount.get() > 0);
                    // ===========================================

                    // wait until everything's been received.
                    for (final long endTime = System.currentTimeMillis() + numThreads * baseTimeoutMillis; endTime > System.currentTimeMillis()
                            && receiver.receivedStringMessages.size() != numThreads;)
                        Thread.sleep(1);
                    // that the right messages were received will be checked later.

                    // ===========================================
                    // just to stretch the code coverage, make the listener throw an exception
                    long numMessages = receiver.numMessages.get();
                    receiver.throwThisOnce.set(new RuntimeException("Yo!"));
                    for (final long endTime = System.currentTimeMillis() + numThreads * baseTimeoutMillis; endTime > System.currentTimeMillis()
                            && receiver.numMessages.get() <= numMessages;)
                        Thread.sleep(1);
                    assertTrue(receiver.numMessages.get() > numMessages);

                    // now make sure we're still receiving messages by making sure the count is STILL going up.
                    numMessages = receiver.numMessages.get();
                    for (final long endTime = System.currentTimeMillis() + numThreads * baseTimeoutMillis; endTime > System.currentTimeMillis()
                            && receiver.numMessages.get() <= numMessages;)
                        Thread.sleep(1);
                    assertTrue(receiver.numMessages.get() > numMessages);
                    // ===========================================

                    assertEquals(0L, statsCollector.getMessagesNotSentCount());

                    // pull the rug out on the adaptor
                    adaptor.stop();

                    // wait until the total number of failed messages == 10 * numThreads
                    assertTrue(TestUtils.poll(baseTimeoutMillis, statsCollector, new TestUtils.Condition<BasicStatsCollector>() {
                        @Override
                        public boolean conditionMet(final BasicStatsCollector o) throws Throwable {
                            return o.getMessagesNotSentCount() > 10 * numThreads;
                        }
                    }));

                    // now stop the senders.
                    for (final SenderRunnable sender : senders)
                        sender.keepGoing.set(false);

                    // ===========================================
                    // wait until all threads are stopped
                    for (final Thread t : threadsToJoinOn)
                        t.join(numThreads * baseTimeoutMillis);

                    // make sure everything actually stopped.
                    for (final SenderRunnable sender : senders)
                        assertTrue(sender.isStopped.get());
                    // ===========================================

                    // ===========================================
                    // Now check to see that we recieved everything we expected.
                    assertEquals(numThreads, receiver.receivedStringMessages.size());
                    for (int i = 0; i < numThreads; i++)
                        assertTrue(receiver.receivedStringMessages.contains("Hello from " + i));
                    // ===========================================
                }
            }

            @Override
            public String toString() {
                return "transportMultipleConnectionsFailedWriter";
            }
        });
    }

    @Test
    public void testBoundedOutputQueue() throws Throwable {
        runAllCombinations(new Checker() {
            @Override
            public void check(final int port, final boolean localhost, final long batchOutgoingMessagesDelayMillis) throws Throwable {
                SenderFactory factory = null;
                TcpReceiver adaptor = null;
                final byte[] message = new byte[1024 * 8];
                final BasicStatsCollector statsCollector = new BasicStatsCollector();
                try {
                    // ===========================================
                    // setup the sender and receiver
                    adaptor = makeHangingReceiver();

                    adaptor.setStatsCollector(statsCollector);
                    final StringListener receiver = new StringListener();
                    adaptor.setListener(receiver);
                    factory = makeSenderFactory(false, statsCollector, batchOutgoingMessagesDelayMillis);

                    if (port > 0)
                        adaptor.setPort(port);
                    if (localhost)
                        adaptor.setUseLocalhost(localhost);
                    // ===========================================

                    adaptor.start(); // start the adaptor
                    final Destination destination = adaptor.getDestination(); // get the destination

                    // send a message
                    final TcpSender sender = (TcpSender) factory.getSender(destination);
                    final int maxQueuedMessages = 100;

                    sender.setMaxNumberOfQueuedMessages(maxQueuedMessages);

                    assertTrue(TestUtils.poll(baseTimeoutMillis, statsCollector, new TestUtils.Condition<BasicStatsCollector>() {
                        @Override
                        public boolean conditionMet(final BasicStatsCollector o) throws Throwable {
                            // this should eventually fail
                            sender.send(message); // this should work
                            return sender.sendingQueue.size() > (maxQueuedMessages * 2);
                        }
                    }));

                    Thread.sleep(100);

                    final int backup = sender.sendingQueue.size();

                    sender.socketTimeout.disrupt(); // kick it.

                    // wait for it to fail
                    assertTrue(TestUtils.poll(baseTimeoutMillis, statsCollector, new TestUtils.Condition<BasicStatsCollector>() {
                        @Override
                        public boolean conditionMet(final BasicStatsCollector o) throws Throwable {
                            return o.getMessagesNotSentCount() > ((maxQueuedMessages * 2) - maxQueuedMessages);
                        }
                    }));

                    // Thread.sleep(100);

                    logger.info("there are " + backup + " message baked up, and " +
                            statsCollector.getMessagesNotSentCount() + " have been discarded.");

                    // This cannot be determined reliably on Cloudbees which is usually too busy
                    // assertTrue("backup is " + backup + " and discarded messages is " + statsCollector.getMessagesNotSentCount(),
                    // statsCollector.getMessagesNotSentCount() < backup);

                    final Long numMessagesReceived = receiver.numMessages.get();
                    assertTrue(TestUtils.poll(baseTimeoutMillis, numMessagesReceived, new TestUtils.Condition<Long>() {
                        @Override
                        public boolean conditionMet(final Long o) throws Throwable {
                            sender.send("Hello".getBytes()); // this should work
                            return receiver.numMessages.get() > o.longValue();
                        }
                    }));
                } finally {
                    if (factory != null)
                        factory.stop();

                    if (adaptor != null)
                        adaptor.stop();
                }

            }

            @Override
            public String toString() {
                return "testBoundedOutputQueue";
            }
        });

    }

    // -------------------------------------------------------------------------------------
    // multirun tests support
    // -------------------------------------------------------------------------------------

    static class SenderRunnable implements Runnable {
        int threadInstance;
        Destination destination;
        SenderFactory senderFactory;

        AtomicBoolean stopOnNormalFailure = new AtomicBoolean(false);
        AtomicBoolean totalFailure = new AtomicBoolean(false);
        AtomicBoolean isStopped = new AtomicBoolean(false);
        AtomicBoolean sendFailed = new AtomicBoolean(false);
        AtomicBoolean keepGoing = new AtomicBoolean(true);
        AtomicLong sentMessageCount = new AtomicLong(0);

        SenderRunnable(final Destination destination, final int threadInstance, final SenderFactory factory) {
            this.destination = destination;
            this.threadInstance = threadInstance;
            this.senderFactory = factory;
        }

        SenderRunnable stopOnNormalFailure(final boolean val) {
            stopOnNormalFailure.set(val);
            return this;
        }

        @Override
        public void run() {
            boolean justGetOut = false;
            while (keepGoing.get() && !justGetOut) {
                try {
                    final Sender sender = senderFactory.getSender(destination);
                    sender.send(("Hello from " + threadInstance).getBytes());
                    sentMessageCount.incrementAndGet();
                } catch (final MessageTransportException e) {
                    sendFailed.set(true);
                    // this means the send got shut down.
                    if (stopOnNormalFailure.get())
                        justGetOut = true; // leave the loop
                } catch (final Throwable th) {
                    totalFailure.set(true);
                }
                Thread.yield();
            }

            isStopped.set(true);
        }
    }

    static class StringListener implements Listener {
        Set<String> receivedStringMessages = new HashSet<String>();
        AtomicReference<RuntimeException> throwThisOnce = new AtomicReference<RuntimeException>();
        AtomicLong numMessages = new AtomicLong(0);
        Object latch = null;
        AtomicLong numIn = new AtomicLong(0);

        public StringListener() {}

        public StringListener(final Object latch) {
            this.latch = latch;
        }

        @Override
        public boolean onMessage(final byte[] messageBytes, final boolean failfast) throws MessageTransportException {
            try {
                numIn.incrementAndGet();
                if (latch != null) {
                    synchronized (latch) {
                        try {
                            latch.wait();
                        } catch (final InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                synchronized (this) {
                    receivedStringMessages.add(new String(messageBytes));
                }
                numMessages.incrementAndGet();
                if (throwThisOnce.get() != null)
                    throw throwThisOnce.getAndSet(null);
                return true;
            } finally {
                numIn.decrementAndGet();
            }
        }

        @Override
        public void shuttingDown() {}
    }

    private static class DisruptibleOutputStream extends OutputStream {
        OutputStream proxied;
        Socket socket;
        int dc = -1;
        int numBytesWritten = 0;

        public DisruptibleOutputStream(final OutputStream proxied, final Socket socket) {
            this.proxied = proxied;
            this.socket = socket;
            this.dc = disruptionCount;
        }

        @Override
        public void write(final int b) throws IOException {
            if (dc >= 0) {
                if (numBytesWritten == dc) {
                    try {
                        socket.close();
                    } catch (final Throwable e) {
                        e.printStackTrace();
                    }
                }
            }

            proxied.write(b);
            numBytesWritten++;
        }

        @Override
        public void flush() throws IOException {
            proxied.flush();
        }

        @Override
        public void close() throws IOException {
            proxied.close();
        }

    }

    private static int disruptionCount = 5; // passed via side effect
    private static boolean onlyOnce = true; // passed via side effect

    // make sure these are reset between every test
    @Before
    public void resetGlobals() {
        disruptionCount = 5;
        onlyOnce = true;
    }

    public static interface OutputStreamFactory {
        public OutputStream makeOutputStream(OutputStream socketOutputStream, Socket socket) throws IOException;
    }

    private TcpSenderFactory makeSenderFactory(final OutputStreamFactory osfactory, final StatsCollector statsCollector,
            final long batchOutgoingMessagesDelayMillis) {
        return osfactory != null ? new TcpSenderFactory(statsCollector, maxOutboundQueueSize, 10000, batchOutgoingMessagesDelayMillis) {
            @Override
            protected TcpSender makeTcpSender(final TcpDestination destination) throws MessageTransportException {
                return new TcpSender(destination, statsCollector, batching, maxNumberOfQueuedOutbound, socketWriteTimeoutMillis,
                        batchOutgoingMessagesDelayMillis, TcpTransport.defaultMtu) {
                    boolean onceOnly = onlyOnce;
                    boolean didItOnceAlready = false;

                    @Override
                    protected synchronized Socket makeSocket(final TcpDestination destination) throws IOException {
                        if (!didItOnceAlready || !onceOnly) {
                            didItOnceAlready = true;
                            return new Socket(destination.inetAddress, destination.port) {
                                final OutputStreamFactory factory = osfactory;

                                @Override
                                public OutputStream getOutputStream() throws IOException {
                                    return factory.makeOutputStream(super.getOutputStream(), this);
                                }
                            };
                        } else // we either did it onceAlready and onceOnly was set
                            return new Socket(destination.inetAddress, destination.port);
                    }
                };
            }
        } : new TcpSenderFactory(statsCollector, maxOutboundQueueSize, 10000, batchOutgoingMessagesDelayMillis);
    }

    private TcpSenderFactory makeSenderFactory(final boolean disruptible, final StatsCollector statsCollector,
            final long batchOutgoingMessagesDelayMillis) {
        return disruptible ? makeSenderFactory(new OutputStreamFactory() {
            @Override
            public OutputStream makeOutputStream(final OutputStream socketOutputStream, final Socket socket) throws IOException {
                return new DisruptibleOutputStream(socketOutputStream, socket);
            }
        }, statsCollector, batchOutgoingMessagesDelayMillis)
                : new TcpSenderFactory(statsCollector, maxOutboundQueueSize, 10000, batchOutgoingMessagesDelayMillis);
    }

    @Test
    public void transportMTWorkerMessage() throws Throwable {
        runAllCombinations(new Checker() {
            @Override
            public void check(final int port, final boolean localhost, final long batchOutgoingMessagesDelayMillis) throws Throwable {
                try (final BasicStatsCollector statsCollector = new BasicStatsCollector();
                        final SenderFactory factory = makeSenderFactory(false, statsCollector, batchOutgoingMessagesDelayMillis); // distruptible sender factory
                        final TcpReceiver adaptor = new TcpReceiver(null, getFailFast());) {
                    // ===========================================
                    // setup the sender and receiver
                    final Object latch = new Object();
                    adaptor.setStatsCollector(statsCollector);
                    final StringListener receiver = new StringListener(latch);
                    adaptor.setListener(receiver);

                    if (port > 0)
                        adaptor.setPort(port);
                    if (localhost)
                        adaptor.setUseLocalhost(localhost);
                    // ===========================================

                    adaptor.start(); // start the adaptor
                    final Destination destination = adaptor.getDestination(); // get the destination

                    // send a message
                    final Sender sender = factory.getSender(destination);

                    final int numAdaptorThreads = adaptor.executor.getNumThreads();

                    assertTrue(numAdaptorThreads > 1);

                    final DefaultDempsyExecutor executor = ((DefaultDempsyExecutor) adaptor.executor);

                    // first dump the max number of messages
                    for (int i = 0; i < executor.getMaxNumberOfQueuedLimitedTasks(); i++)
                        sender.send("Hello".getBytes());

                    // wait until there's room in the queue due to messages being passed on to
                    // the receiver; one for each thread.
                    assertTrue(TestUtils.poll(baseTimeoutMillis, receiver, new TestUtils.Condition<StringListener>() {
                        @Override
                        public boolean conditionMet(final StringListener o) {
                            return o.numIn.get() == numAdaptorThreads;
                        }
                    }));

                    // send one more for each opened up place in the queue now
                    for (int i = 0; i < numAdaptorThreads; i++)
                        sender.send("Hello".getBytes());

                    // wait until all Listeners are in and all threads enqueued. This is the totally full state.
                    assertTrue(TestUtils.poll(baseTimeoutMillis, ((DefaultDempsyExecutor) adaptor.executor),
                            new TestUtils.Condition<DefaultDempsyExecutor>() {
                                @Override
                                public boolean conditionMet(final DefaultDempsyExecutor o) {
                                    return o.getNumberLimitedPending() == o.getMaxNumberOfQueuedLimitedTasks();
                                }
                            }));

                    assertEquals(0, statsCollector.getDiscardedMessageCount());

                    // we are going to poll but we are going to keep adding to the queu of tasks. So we add 2, let one go,
                    // until we start seeing rejects.

                    assertTrue(TestUtils.poll(baseTimeoutMillis, statsCollector,
                            new TestUtils.Condition<BasicStatsCollector>() {
                                @Override
                                public boolean conditionMet(final BasicStatsCollector o) throws Throwable {
                                    sender.send("Hello".getBytes());
                                    sender.send("Hello".getBytes());

                                    synchronized (latch) {
                                        latch.notify(); // single exit.
                                    }

                                    return o.getDiscardedMessageCount() > 0;
                                }
                            }));

                    receiver.latch = null;
                    synchronized (latch) {
                        latch.notifyAll();
                    }

                    adaptor.stop();

                    synchronized (latch) {
                        latch.notifyAll();
                    }
                }
            }

            @Override
            public String toString() {
                return "transportMTWorkerMessage";
            }
        });
    }

}
