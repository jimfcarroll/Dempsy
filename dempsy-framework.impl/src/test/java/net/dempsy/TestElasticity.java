package net.dempsy;

import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.cluster.ClusterInfoSession;
import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;

public class TestElasticity extends DempsyBaseTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestElasticity.class);

    private static final int profilerTestNumberCount = 100000;

    public static final String[][] actxPath = {
            { "elasticity/adaptor.xml", },
            { "elasticity/mp-num-count.xml", },
            { "elasticity/mp-num-count.xml", },
            { "elasticity/mp-num-count.xml", },
            { "elasticity/mp-num-rank.xml", },
    };

    public TestElasticity(final String routerId, final String containerId, final String sessCtx) {
        super(LOGGER, routerId, containerId, sessCtx);
    }

    @Before
    public void reset() {
        NumberCounter.messageCount.set(0);
    }

    // ========================================================================
    // Test classes we will be working with. The old word count example modified.
    // ========================================================================
    @MessageType
    public static class Number implements Serializable {
        private static final long serialVersionUID = 1L;
        private Integer number;
        private int rankIndex;

        public Number() {} // needed for kryo-serializer

        public Number(final Integer number, final int rankIndex) {
            this.number = number;
            this.rankIndex = rankIndex;
        }

        @MessageKey
        public Integer getNumber() {
            return number;
        }

        @Override
        public String toString() {
            return "" + number + "[" + rankIndex + "]";
        }
    }

    @MessageType
    public static class VerifyNumber extends Number implements Serializable {
        private static final long serialVersionUID = 1L;

        public VerifyNumber() {}

        public VerifyNumber(final Integer number, final int rankIndex) {
            super(number, rankIndex);
        }

        @Override
        public String toString() {
            return "verifying " + super.toString();
        }
    }

    @MessageType
    public static class NumberCount implements Serializable {
        private static final long serialVersionUID = 1L;
        public Integer number;
        public long count;
        public int rankIndex;

        public NumberCount(final Number number, final long count) {
            this.number = number.getNumber();
            this.count = count;
            this.rankIndex = number.rankIndex;
        }

        public NumberCount() {} // for kryo

        static final Integer one = new Integer(1);

        @MessageKey
        public Integer getKey() {
            return number;
        }

        @Override
        public String toString() {
            return "(" + count + " " + number + "s)[" + rankIndex + "]";
        }
    }

    public static class NumberProducer implements Adaptor {
        public Dispatcher dispatcher = null;

        @Override
        public void setDispatcher(final Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Override
        public void start() {}

        @Override
        public void stop() {}
    }

    @Mp
    public static class NumberCounter implements Cloneable {
        public static AtomicLong messageCount = new AtomicLong(0);
        long counter = 0;
        String wordText;

        @Activation
        public void initMe(final String key) {
            this.wordText = key;
        }

        @MessageHandler
        public NumberCount handle(final Number word) {
            LOGGER.trace("NumberCount recevied {}", word);
            messageCount.incrementAndGet();
            return new NumberCount(word, counter++);
        }

        @Override
        public NumberCounter clone() throws CloneNotSupportedException {
            return (NumberCounter) super.clone();
        }
    }

    public static class Rank {
        public final Integer number;
        public final Long rank;

        public Rank(final Integer number, final long rank) {
            this.number = number;
            this.rank = rank;
        }

        @Override
        public String toString() {
            return "[ " + number + " count:" + rank + " ]";
        }
    }

    @Mp
    public static class NumberRank implements Cloneable {
        public final AtomicLong totalMessages = new AtomicLong(0);

        @SuppressWarnings({ "unchecked", "rawtypes" })
        final public AtomicReference<Map<Integer, Long>[]> countMap = new AtomicReference(new Map[1000]);

        {
            for (int i = 0; i < 1000; i++)
                countMap.get()[i] = new ConcurrentHashMap<Integer, Long>();
        }

        @MessageHandler
        public void handle(final NumberCount wordCount) {
            LOGGER.trace("NumberRank received {}", wordCount);
            totalMessages.incrementAndGet();
            countMap.get()[wordCount.rankIndex].put(wordCount.number, wordCount.count);
        }

        @Override
        public NumberRank clone() throws CloneNotSupportedException {
            return (NumberRank) super.clone();
        }

        public List<Rank> getPairs(final int rankIndex) {
            final List<Rank> ret = new ArrayList<>(countMap.get()[rankIndex].size() + 10);
            for (final Map.Entry<Integer, Long> cur : countMap.get()[rankIndex].entrySet())
                ret.add(new Rank(cur.getKey(), cur.getValue()));
            Collections.sort(ret, (o1, o2) -> o2.rank.compareTo(o1.rank));
            return ret;
        }
    }

    // ========================================================================

    @Test
    public void testForProfiler() throws Throwable {
        // set up the test.
        final Number[] numbers = new Number[profilerTestNumberCount];
        final Random random = new Random();
        for (int i = 0; i < numbers.length; i++)
            numbers[i] = new Number(random.nextInt(1000), 0);

        final KeyExtractor ke = new KeyExtractor();

        runCombos((r, c, s) -> "microshard".equals(r), actxPath, ns -> {
            final List<NodeManagerWithContext> nodes = ns.nodes;
            LOGGER.trace("==== Starting ...");

            // Grab the one NumberRank Mp from the single Node in the third (0 base 2nd) cluster.
            final NumberRank rank = nodes.get(4).ctx.getBean(NumberRank.class);

            try (final ClusterInfoSession session = ns.sessionFactory.createSession();) {
                waitForEvenShardDistribution(session, "test-cluster1", 3);

                // grab the adaptor from the 0'th cluster + the 0'th (only) node.
                final NumberProducer adaptor = nodes.get(0).ctx.getBean(NumberProducer.class);

                // grab access to the Dispatcher from the Adaptor
                final Dispatcher dispatcher = adaptor.dispatcher;

                final long startTime = System.currentTimeMillis();

                for (int i = 0; i < numbers.length; i++)
                    dispatcher.dispatch(ke.extract(numbers[i]));

                LOGGER.trace("====> Checking exact count.");

                // keep going as long as they are trickling in.
                long lastNumberOfMessages = -1;
                while (rank.totalMessages.get() > lastNumberOfMessages) {
                    lastNumberOfMessages = rank.totalMessages.get();
                    if (poll(rank.totalMessages, o -> o.get() == profilerTestNumberCount))
                        break;
                }

                LOGGER.trace("testForProfiler time " + (System.currentTimeMillis() - startTime));

                assertEquals(profilerTestNumberCount, rank.totalMessages.get());
                assertEquals(profilerTestNumberCount, NumberCounter.messageCount.get());
            }
        });
    }

    // static private TestUtils.Condition<Thread> threadStoppedCondition = new TestUtils.Condition<Thread>() {
    // @Override
    // public boolean conditionMet(Thread o) {
    // return !o.isAlive();
    // }
    // };
    //
    // private void verifyMessagesThrough(final AtomicInteger rankIndexToSend,
    // final AtomicBoolean keepGoing, final Runnable sendMessages, final NumberRank rank,
    // final ClassPathXmlApplicationContext[] contexts, final int indexOfNumberCountMp,
    // final Dispatcher dispatcher, final int width, final ClassPathXmlApplicationContext... additionalContexts) throws Throwable {
    // rankIndexToSend.incrementAndGet();
    // keepGoing.set(true);
    // final Thread tmpThread = new Thread(sendMessages);
    // tmpThread.start();
    //
    // // wait for the messages to get all the way through
    // assertTrue(TestUtils.poll(baseTimeoutMillis, rank, new TestUtils.Condition<NumberRank>() {
    // @Override
    // public boolean conditionMet(NumberRank rank) {
    // return rank.countMap.get()[rankIndexToSend.get()].size() == 20;
    // }
    // }));
    // keepGoing.set(false);
    //
    // // wait for the thread to exit.
    // assertTrue(TestUtils.poll(baseTimeoutMillis, tmpThread, threadStoppedCondition));
    //
    // // This only works when the container blocks and after the output's are set up from the previous
    // // other thread run.
    // if (!TestUtils.getReceiver(indexOfNumberCountMp, contexts).getFailFast()) {
    // logger.trace("++++ Sending through 20 messages.");
    //
    // // OK ... the sender thread has overwhelmed everything and we need to wait until message top being processed.
    // long curTotalMessages = rank.totalMessages.get();
    // long prevTotalMessages = 0;
    // while (curTotalMessages != prevTotalMessages) {
    // logger.trace("++++ Waiting for all messages to finish " + prevTotalMessages + ", " + curTotalMessages);
    // Thread.sleep(1000);
    // prevTotalMessages = curTotalMessages;
    // curTotalMessages = rank.totalMessages.get();
    // }
    //
    // // now that all of the through details are there, we want to clear the rank map and send a single
    // // value per slot back through.
    // rankIndexToSend.incrementAndGet();
    // logger.trace("Verifying rank index of " + rankIndexToSend.get());
    // for (int num = 0; num < 20; num++)
    // dispatcher.dispatch(new VerifyNumber(num, rankIndexToSend.get()));
    //
    // // all 20 should make it to the end.
    // TestUtils.poll(baseTimeoutMillis, rank, new TestUtils.Condition<NumberRank>() {
    // @Override
    // public boolean conditionMet(NumberRank rank) {
    // return rank.countMap.get()[rankIndexToSend.get()].size() == 20;
    // }
    // });
    //
    // assertEquals(20, rank.countMap.get()[rankIndexToSend.get()].size());
    // }
    //
    // final List<ClassPathXmlApplicationContext> tmpl = new ArrayList<ClassPathXmlApplicationContext>(contexts.length + additionalContexts.length);
    // tmpl.addAll(Arrays.asList(contexts));
    // tmpl.addAll(Arrays.asList(additionalContexts));
    // checkMpDistribution("test-cluster1", tmpl.toArray(new ClassPathXmlApplicationContext[0]), 20, width);
    // }
    //
    // @SuppressWarnings("unchecked")
    // @Test
    // public void testNumberCountDropOneAndReAdd() throws Throwable {
    // runAllCombinations(new Checker() {
    // @Override
    // public void check(final ClassPathXmlApplicationContext[] contexts) throws Throwable {
    // // keepGoing is for the separate thread that pumps messages into the system.
    // final AtomicBoolean keepGoing = new AtomicBoolean(true);
    // try {
    // logger.trace("==== <- Starting");
    //
    // // grab the adaptor from the 0'th cluster + the 0'th (only) node.
    // final NumberProducer adaptor = (NumberProducer) TestUtils.getAdaptor(0, contexts);
    //
    // // grab access to the Dispatcher from the Adaptor
    // final Dispatcher dispatcher = adaptor.dispatcher;
    //
    // // This is a Runnable that will pump messages to the dispatcher until keepGoing is
    // // flipped to 'false.' It's stateless so it can be reused as needed.
    // final AtomicInteger rankIndexToSend = new AtomicInteger(0);
    // final Runnable sendMessages = new Runnable() {
    // @Override
    // public void run() {
    // // send a few numbers. There are 20 shards so in order to cover all
    // // shards we can send in 20 messages. It just so happens that the hashCode
    // // for an integer is the integer itself so we can get every shard by sending
    // while (keepGoing.get())
    // for (int num = 0; num < 20; num++)
    // dispatcher.dispatch(new Number(num, rankIndexToSend.get()));
    // }
    // };
    //
    // assertTrue(TestUtils.waitForClusterBalance(baseTimeoutMillis, "test-cluster1", contexts, 20, 3));
    //
    // // Grab the one NumberRank Mp from the single Node in the third (0 base 2nd) cluster.
    // final NumberRank rank = (NumberRank) TestUtils.getMp(4, contexts);
    //
    // verifyMessagesThrough(rankIndexToSend, keepGoing, sendMessages, rank, contexts, 1, dispatcher, 3);
    //
    // // now kill a node.
    // final Dempsy middleDempsy = TestUtils.getDempsy(2, contexts); // select the middle Dempsy
    // logger.trace("==== Stopping middle Dempsy servicing shards " + TestUtils.getNode(2, contexts).strategyInbound);
    // middleDempsy.stop();
    // assertTrue(middleDempsy.waitToBeStopped(baseTimeoutMillis));
    // logger.trace("==== Stopped middle Dempsy");
    //
    // assertTrue(TestUtils.waitForClusterBalance(baseTimeoutMillis, "test-cluster1", contexts, 20, 2));
    //
    // verifyMessagesThrough(rankIndexToSend, keepGoing, sendMessages, rank, contexts, 1, dispatcher, 2);
    //
    // // now, bring online another instance.
    // logger.trace("==== starting a new one");
    // final ClassPathXmlApplicationContext actx = startAnotherNode(2);
    // assertTrue(TestUtils.waitForClusterBalance(baseTimeoutMillis, "test-cluster1", contexts, 20, 3, actx));
    //
    // verifyMessagesThrough(rankIndexToSend, keepGoing, sendMessages, rank, contexts, 1, dispatcher, 3, actx);
    // } finally {
    // keepGoing.set(false);
    // logger.trace("==== Exiting test.");
    // }
    // }
    //
    // @Override
    // public String toString() {
    // return "testNumberCountDropOneAndReAdd";
    // }
    //
    // @Override
    // public void setup() {
    // DempsyTestBase.TestKryoOptimizer.proxy = NumberCounterKryoOptimizer.instance;
    // }
    // },
    // new Pair<String[], String>(new String[] { actxPath }, "test-cluster0"), // adaptor
    // new Pair<String[], String>(new String[] { actxPath }, "test-cluster1"), // 3 NumberCounter Mps
    // new Pair<String[], String>(new String[] { actxPath }, "test-cluster1"), // .
    // new Pair<String[], String>(new String[] { actxPath }, "test-cluster1"), // .
    // new Pair<String[], String>(new String[] { actxPath }, "test-cluster2"));// NumberRank
    // }
    //
    // @SuppressWarnings("unchecked")
    // @Test
    // public void testNumberCountAddOneThenDrop() throws Throwable {
    // runAllCombinations(new Checker() {
    // @Override
    // public void check(final ClassPathXmlApplicationContext[] contexts) throws Throwable {
    // // keepGoing is for the separate thread that pumps messages into the system.
    // final AtomicBoolean keepGoing = new AtomicBoolean(true);
    // try {
    // logger.trace("==== <- Starting");
    //
    // // grab the adaptor from the 0'th cluster + the 0'th (only) node.
    // final NumberProducer adaptor = (NumberProducer) TestUtils.getAdaptor(0, contexts);
    //
    // // grab access to the Dispatcher from the Adaptor
    // final Dispatcher dispatcher = adaptor.dispatcher;
    //
    // // This is a Runnable that will pump messages to the dispatcher until keepGoing is
    // // flipped to 'false.' It's stateless so it can be reused as needed.
    // final AtomicInteger rankIndexToSend = new AtomicInteger(0);
    // final Runnable sendMessages = new Runnable() {
    // @Override
    // public void run() {
    // // send a few numbers. There are 20 shards so in order to cover all
    // // shards we can send in 20 messages. It just so happens that the hashCode
    // // for an integer is the integer itself so we can get every shard by sending
    // while (keepGoing.get())
    // for (int num = 0; num < 20; num++)
    // dispatcher.dispatch(new Number(num, rankIndexToSend.get()));
    // }
    // };
    //
    // assertTrue(TestUtils.waitForClusterBalance(baseTimeoutMillis, "test-cluster1", contexts, 20, 2));
    //
    // // Grab the one NumberRank Mp from the single Node in the third (0 base 2nd) cluster.
    // final NumberRank rank = (NumberRank) TestUtils.getMp(3, contexts);
    //
    // verifyMessagesThrough(rankIndexToSend, keepGoing, sendMessages, rank, contexts, 1, dispatcher, 2);
    //
    // // now, bring online another instance.
    // logger.trace("==== starting a new one");
    // final ClassPathXmlApplicationContext actx = startAnotherNode(2);
    // assertTrue(TestUtils.waitForClusterBalance(baseTimeoutMillis, "test-cluster1", contexts, 20, 3, actx));
    //
    // verifyMessagesThrough(rankIndexToSend, keepGoing, sendMessages, rank, contexts, 1, dispatcher, 3, actx);
    //
    // // now kill a node.
    // final Dempsy middleDempsy = TestUtils.getDempsy(2, contexts); // select the middle Dempsy
    // logger.trace("==== Stopping middle Dempsy servicing shards " + TestUtils.getNode(2, contexts).strategyInbound);
    // middleDempsy.stop();
    // assertTrue(middleDempsy.waitToBeStopped(baseTimeoutMillis));
    // logger.trace("==== Stopped middle Dempsy");
    //
    // assertTrue(TestUtils.waitForClusterBalance(baseTimeoutMillis, "test-cluster1", contexts, 20, 2, actx));
    //
    // verifyMessagesThrough(rankIndexToSend, keepGoing, sendMessages, rank, contexts, 1, dispatcher, 2, actx);
    // } finally {
    // keepGoing.set(false);
    // logger.trace("==== Exiting test.");
    // }
    // }
    //
    // @Override
    // public String toString() {
    // return "testNumberCountDropOneAndReAdd";
    // }
    //
    // @Override
    // public void setup() {
    // DempsyTestBase.TestKryoOptimizer.proxy = NumberCounterKryoOptimizer.instance;
    // }
    // },
    // new Pair<String[], String>(new String[] { actxPath }, "test-cluster0"), // adaptor
    // new Pair<String[], String>(new String[] { actxPath }, "test-cluster1"), // 2 NumberCounter Mps
    // new Pair<String[], String>(new String[] { actxPath }, "test-cluster1"), // .
    // new Pair<String[], String>(new String[] { actxPath }, "test-cluster2"));// NumberRank
    // }
    //
    // /**
    // * Verify that the shards are correctly balanced for the given cluster and that the
    // * given expectedNumberOfNodes are available.
    // */
    // private void checkMpDistribution(final String cluster, final ClassPathXmlApplicationContext[] contexts, final int totalNumberOfShards,
    // final int expectedNumberOfNodes) throws Throwable {
    // final List<Dempsy.Application.Cluster.Node> nodes = TestUtils.getNodes(contexts, cluster);
    // final List<Container> containers = new ArrayList<Container>(nodes.size());
    //
    // for (final Dempsy.Application.Cluster.Node node : nodes) {
    // final Dempsy dempsy = node.yspmeDteg();
    // if (dempsy.isRunning())
    // containers.add(node.getMpContainer());
    // }
    //
    // assertEquals(expectedNumberOfNodes, containers.size());
    //
    // final int minNumberOfShards = (int) Math.floor((double) totalNumberOfShards / (double) expectedNumberOfNodes);
    // final int maxNumberOfShards = (int) Math.ceil((double) totalNumberOfShards / (double) expectedNumberOfNodes);
    //
    // int totalNum = 0;
    // for (final Container cur : containers) {
    // final ContainerTestAccess container = (ContainerTestAccess) cur;
    // final int curNum = container.getProcessorCount();
    // assertTrue("" + curNum + " <= " + maxNumberOfShards, curNum <= maxNumberOfShards);
    // assertTrue("" + curNum + " >= " + maxNumberOfShards, curNum >= minNumberOfShards);
    // totalNum += curNum;
    // }
    //
    // assertEquals(totalNumberOfShards, totalNum);
    // }
}
