package net.dempsy.transport.tcp;

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
import net.dempsy.transport.Receiver;
import net.dempsy.transport.RoutedMessage;
import net.dempsy.transport.Sender;
import net.dempsy.transport.SenderFactory;
import net.dempsy.transport.tcp.netty.NettyReceiver;
import net.dempsy.transport.tcp.netty.NettySenderFactory;
import net.dempsy.transport.tcp.nio.NioReceiver;
import net.dempsy.transport.tcp.nio.NioSenderFactory;
import net.dempsy.util.TestInfrastructure;

@RunWith(Parameterized.class)
public class TcpTransportTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TcpTransportTest.class);

    private final Supplier<SenderFactory> senderFactory;

    private final Supplier<AbstractTcpReceiver<?, ?>> receiver;

    public TcpTransportTest(final String senderFactoryName, final Supplier<SenderFactory> senderFactory, final String receiverName,
            final Supplier<AbstractTcpReceiver<?, ?>> receiver) {
        this.senderFactory = senderFactory;
        this.receiver = receiver;
    }

    @Parameters(name = "{index}: senderfactory={0}, receiver={2}")
    public static Collection<Object[]> combos() {
        final Supplier<Receiver> nior = () -> new NioReceiver<>(new JsonSerializer());
        final Supplier<Receiver> nettyr = () -> new NettyReceiver<>(new JsonSerializer());
        return Arrays.asList(new Object[][] {
                { "nio", (Supplier<SenderFactory>) () -> new NioSenderFactory(), "nio", nior },
                { "netty", (Supplier<SenderFactory>) () -> new NettySenderFactory(), "netty", nettyr },
                { "nio", (Supplier<SenderFactory>) () -> new NioSenderFactory(), "netty", nettyr },
                { "netty", (Supplier<SenderFactory>) () -> new NettySenderFactory(), "nio", nior },
        });

    }

    @Test
    public void testReceiverStart() throws Exception {
        final AtomicBoolean resolverCalled = new AtomicBoolean(false);
        try (ServiceTracker tr = new ServiceTracker();) {
            final AbstractTcpReceiver<?, ?> r = tr.track(receiver.get())
                    .setResolver(a -> {
                        resolverCalled.set(true);
                        return a;
                    }).setNumHandlers(2)
                    .setUseLocalHost(true);

            final TcpAddress addr = r.getAddress();
            LOGGER.debug(addr.toString());
            r.start(null, tr.track(new DefaultThreadingModel(TcpTransportTest.class.getSimpleName() + ".testReceiverStart")));
            assertTrue(resolverCalled.get());
        }
    }

    @Test
    public void testMessage() throws Exception {
        try (ServiceTracker tr = new ServiceTracker();) {
            final AbstractTcpReceiver<?, ?> r = tr.track(receiver.get())
                    .setNumHandlers(2)
                    .setUseLocalHost(true);

            final TcpAddress addr = r.getAddress();
            LOGGER.debug(addr.toString());
            final AtomicReference<RoutedMessage> rm = new AtomicReference<>(null);
            r.start((Listener<RoutedMessage>) msg -> {
                rm.set(msg);
                return true;
            }, tr.track(new DefaultThreadingModel(TcpTransportTest.class.getSimpleName() + ".testReceiverStart")));

            try (final SenderFactory sf = senderFactory.get();) {
                sf.start(new TestInfrastructure(null, null) {
                    @Override
                    public String getNodeId() {
                        return "test";
                    }
                });
                final Sender sender = sf.getSender(addr);
                sender.send(new RoutedMessage(new int[] { 0 }, "Hello", "Hello"));

                assertTrue(poll(o -> rm.get() != null));
                assertEquals("Hello", rm.get().message);
            }
        }
    }

    @Test
    public void testLargeMessage() throws Exception {
        final String huge = TestWordCount.readBible();
        try (final ServiceTracker tr = new ServiceTracker();) {
            final AbstractTcpReceiver<?, ?> r = tr.track(receiver.get())
                    .setNumHandlers(2)
                    .setUseLocalHost(true)
                    .setMaxMessageSize(1024 * 1024 * 1024);

            final TcpAddress addr = r.getAddress();
            LOGGER.debug(addr.toString());
            final AtomicReference<RoutedMessage> rm = new AtomicReference<>(null);
            r.start((Listener<RoutedMessage>) msg -> {
                rm.set(msg);
                return true;
            }, tr.track(new DefaultThreadingModel(TcpTransportTest.class.getSimpleName() + ".testReceiverStart")));

            try (final SenderFactory sf = senderFactory.get();) {
                sf.start(new TestInfrastructure(null, null));
                final Sender sender = sf.getSender(addr);
                sender.send(new RoutedMessage(new int[] { 0 }, "Hello", huge));

                assertTrue(poll(o -> rm.get() != null));
                assertEquals(huge, rm.get().message);
            }
        }
    }

    private static final String NUM_SENDER_THREADS = "2";

    private void runMultiMessage(final int numThreads, final int numMessagePerThread, final String message, final Serializer serializer)
            throws Exception {
        try (final ServiceTracker tr = new ServiceTracker();) {
            final AbstractTcpReceiver<?, ?> r = tr.track(receiver.get())
                    .setNumHandlers(2)
                    .setUseLocalHost(true)
                    .setMaxMessageSize(1024 * 1024 * 1024);

            final TcpAddress addr = r.getAddress();
            LOGGER.debug(addr.toString());
            final AtomicLong msgCount = new AtomicLong();
            r.start((Listener<RoutedMessage>) msg -> {
                msgCount.incrementAndGet();
                return true;
            }, tr.track(new DefaultThreadingModel(TcpTransportTest.class.getSimpleName() + ".testReceiverStart")));

            final AtomicBoolean letMeGo = new AtomicBoolean(false);
            final CountDownLatch waitToExit = new CountDownLatch(1);

            final List<Thread> threads = IntStream.range(0, numThreads).mapToObj(threadNum -> new Thread(() -> {
                try (final SenderFactory sf = senderFactory.get();) {
                    sf.start(new TestInfrastructure(null, null) {
                        @Override
                        public Map<String, String> getConfiguration() {
                            final Map<String, String> ret = new HashMap<>();
                            ret.put(sf.getClass().getPackage().getName() + "." + NioSenderFactory.CONFIG_KEY_SENDER_THREADS, NUM_SENDER_THREADS);
                            ret.put(sf.getClass().getPackage().getName() + "." + NettySenderFactory.CONFIG_KEY_SENDER_THREADS, NUM_SENDER_THREADS);
                            return ret;
                        }
                    });
                    final Sender sender = sf.getSender(addr);
                    while (!letMeGo.get())
                        Thread.yield();
                    for (int i = 0; i < numMessagePerThread; i++)
                        sender.send(new RoutedMessage(new int[] { 0 }, "Hello", message));

                    // we need to keep the sender factory going until all messages were accounted for

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
            assertTrue(poll(999999999L, new Long((long) numThreads * (long) numMessagePerThread), v -> {
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

    AtomicLong messageNum = new AtomicLong();

    @Test
    public void testMultiHugeMessage() throws Exception {
        runMultiMessage(1, 500, "" + messageNum.incrementAndGet() + TestWordCount.readBible() + messageNum.incrementAndGet(), new JsonSerializer());
    }
}
