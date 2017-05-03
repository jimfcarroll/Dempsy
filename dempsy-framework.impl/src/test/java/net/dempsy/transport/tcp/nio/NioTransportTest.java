package net.dempsy.transport.tcp.nio;

import static net.dempsy.util.Functional.chain;
import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.ServiceTracker;
import net.dempsy.TestWordCount;
import net.dempsy.serialization.Serializer;
import net.dempsy.serialization.jackson.JsonSerializer;
import net.dempsy.serialization.kryo.KryoSerializer;
import net.dempsy.threading.DefaultThreadingModel;
import net.dempsy.transport.Listener;
import net.dempsy.transport.RoutedMessage;
import net.dempsy.transport.Sender;
import net.dempsy.transport.SenderFactory;
import net.dempsy.transport.tcp.TcpAddress;
import net.dempsy.util.TestInfrastructure;

@RunWith(Parameterized.class)
public class NioTransportTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(NioTransportTest.class);

    private final Supplier<SenderFactory> senderFactory;

    public NioTransportTest(final String senderFactoryName, final Supplier<SenderFactory> senderFactory) {
        this.senderFactory = senderFactory;
    }

    @Parameters(name = "{index}: senderfactory={0}")
    public static Collection<Object[]> combos() {
        return Arrays.asList(new Object[][] {
                { "nio", (Supplier<SenderFactory>) () -> new NioSenderFactory() },
                // { "netty", (Supplier<SenderFactory>) () -> new NettySenderFactory() },
        });
    }

    @Test
    public void testReceiverStart() throws Exception {
        final AtomicBoolean resolverCalled = new AtomicBoolean(false);
        try (ServiceTracker tr = new ServiceTracker();) {
            final NioReceiver<RoutedMessage> r = tr.track(new NioReceiver<RoutedMessage>(new JsonSerializer()))
                    .setNumHandlers(2)
                    .setResolver(a -> {
                        resolverCalled.set(true);
                        return a;
                    }).setUseLocalHost(true);

            final TcpAddress addr = r.getAddress();
            LOGGER.debug(addr.toString());
            r.start(null, tr.track(new DefaultThreadingModel(NioTransportTest.class.getSimpleName() + ".testReceiverStart")));
            assertTrue(resolverCalled.get());
        }
    }

    @Test
    public void testMessage() throws Exception {
        final AtomicBoolean resolverCalled = new AtomicBoolean(false);
        try (ServiceTracker tr = new ServiceTracker();) {
            final NioReceiver<RoutedMessage> r = tr.track(new NioReceiver<RoutedMessage>(new KryoSerializer()))
                    .setNumHandlers(2)
                    .setResolver(a -> {
                        resolverCalled.set(true);
                        return a;
                    }).setUseLocalHost(true);

            final TcpAddress addr = r.getAddress();
            LOGGER.debug(addr.toString());
            final AtomicReference<RoutedMessage> rm = new AtomicReference<>(null);
            r.start((Listener<RoutedMessage>) msg -> {
                rm.set(msg);
                return true;
            }, tr.track(new DefaultThreadingModel(NioTransportTest.class.getSimpleName() + ".testReceiverStart")));

            try (final SenderFactory sf = senderFactory.get();) {
                sf.start(new TestInfrastructure(null, null) {
                    @Override
                    public String getNodeId() {
                        return "test";
                    }
                });
                final Sender sender = sf.getSender(addr);
                sender.send(new RoutedMessage(new int[] { 0 }, "Hello", "Hello"));

                assertTrue(resolverCalled.get());
                assertTrue(poll(o -> rm.get() != null));
                assertEquals("Hello", rm.get().message);
            }
        }
    }

    @Test
    public void testLargeMessage() throws Exception {
        final AtomicBoolean resolverCalled = new AtomicBoolean(false);
        final String huge = TestWordCount.readBible();
        try (ServiceTracker tr = new ServiceTracker();) {
            final NioReceiver<RoutedMessage> r = tr.track(new NioReceiver<RoutedMessage>(new JsonSerializer()))
                    .setNumHandlers(2)
                    .setResolver(a -> {
                        resolverCalled.set(true);
                        return a;
                    }).setUseLocalHost(true)
                    .setMaxMessageSize(1024 * 1024 * 1024);

            final TcpAddress addr = r.getAddress();
            LOGGER.debug(addr.toString());
            final AtomicReference<RoutedMessage> rm = new AtomicReference<>(null);
            r.start((Listener<RoutedMessage>) msg -> {
                rm.set(msg);
                return true;
            }, tr.track(new DefaultThreadingModel(NioTransportTest.class.getSimpleName() + ".testReceiverStart")));

            try (final SenderFactory sf = senderFactory.get();) {
                sf.start(new TestInfrastructure(null, null));
                final Sender sender = sf.getSender(addr);
                sender.send(new RoutedMessage(new int[] { 0 }, "Hello", huge));

                assertTrue(resolverCalled.get());
                assertTrue(poll(o -> rm.get() != null));
                assertEquals(huge, rm.get().message);
            }
        }
    }

    private void runMultiMessage(final int numThreads, final int numMessagePerThread, final String message, final Serializer serializer)
            throws Exception {
        final AtomicBoolean resolverCalled = new AtomicBoolean(false);
        try (final ServiceTracker tr = new ServiceTracker();) {
            final NioReceiver<RoutedMessage> r = tr.track(new NioReceiver<RoutedMessage>(serializer))
                    .setNumHandlers(5)
                    .setResolver(a -> {
                        resolverCalled.set(true);
                        return a;
                    }).setUseLocalHost(true)
                    .setMaxMessageSize(1024 * 1024 * 1024);;

            final TcpAddress addr = r.getAddress();
            LOGGER.debug(addr.toString());
            final AtomicLong msgCount = new AtomicLong();
            r.start((Listener<RoutedMessage>) msg -> {
                msgCount.incrementAndGet();
                return true;
            }, tr.track(new DefaultThreadingModel(NioTransportTest.class.getSimpleName() + ".testReceiverStart")));

            final AtomicBoolean letMeGo = new AtomicBoolean(false);
            final CountDownLatch waitToExit = new CountDownLatch(1);

            final List<Thread> threads = IntStream.range(0, numThreads).mapToObj(threadNum -> new Thread(() -> {
                try (final SenderFactory sf = senderFactory.get();) {
                    sf.start(new TestInfrastructure(null, null) {
                        @Override
                        public Map<String, String> getConfiguration() {
                            final Map<String, String> ret = new HashMap<>();
                            ret.put(sf.getClass().getPackage().getName() + "." + NioSenderFactory.CONFIG_KEY_SENDER_THREADS, "5");
                            return ret;
                        }
                    });
                    final Sender sender = sf.getSender(addr);
                    while (!letMeGo.get())
                        Thread.yield();
                    for (int i = 0; i < numMessagePerThread; i++)
                        sender.send(new RoutedMessage(new int[] { 0 }, "Hello", message));

                    // we need to keep the sender factory going until all messages were accounted for

                    System.out.println("Done!");
                    try {
                        waitToExit.await();
                    } catch (final InterruptedException ie) {}

                }
            }, "testMultiMessage-Sender-" + threadNum))
                    .map(th -> chain(th, t -> t.start()))
                    .collect(Collectors.toList());
            Thread.sleep(10);

            // here's we go.
            letMeGo.set(true);

            // the total number of messages sent should be this count.
            assertTrue(poll(new Long((long) numThreads * (long) numMessagePerThread), v -> {
                System.out.println(v.toString() + " == " + msgCount);
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                return v.longValue() == msgCount.get();
            }));

            // let the threads exit
            waitToExit.countDown();

            // all threads should eventually exit.
            assertTrue(poll(threads, o -> o.stream().filter(t -> t.isAlive()).count() == 0));

        }
    }

    @Test
    public void testMultiMessage() throws Exception {
        runMultiMessage(10, 10000, "Hello", new KryoSerializer());
    }

    @Test
    public void testMultiHugeMessage() throws Exception {
        runMultiMessage(5, 100, TestWordCount.readBible(), new JsonSerializer());
    }
}
