package net.dempsy;

import static net.dempsy.utils.test.ConditionPoll.poll;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import net.dempsy.cluster.local.LocalClusterSessionFactory;
import net.dempsy.config.Node;
import net.dempsy.container.MetricGetters;
import net.dempsy.lifecycle.annotation.Activation;
import net.dempsy.lifecycle.annotation.MessageHandler;
import net.dempsy.lifecycle.annotation.MessageKey;
import net.dempsy.lifecycle.annotation.MessageType;
import net.dempsy.lifecycle.annotation.Mp;
import net.dempsy.lifecycle.annotation.utils.KeyExtractor;
import net.dempsy.messages.Adaptor;
import net.dempsy.messages.Dispatcher;
import net.dempsy.utils.test.SystemPropertyManager;

public class TestWordCount {
    private static Logger LOGGER = LoggerFactory.getLogger(TestWordCount.class);

    public static final String wordResource = "word-count/AV1611Bible.txt.gz";

    @Before
    public void setup() {
        WordProducer.latch = new CountDownLatch(0);
    }

    // ========================================================================
    // Test classes we will be working with. The old word count example.
    // ========================================================================
    @MessageType
    public static class Word implements Serializable {
        private static final long serialVersionUID = 1L;
        private String word;

        public Word() {} // needed for kryo-serializer

        public Word(final String word) {
            this.word = word;
        }

        @MessageKey
        public String getWord() {
            return word;
        }
    }

    @MessageType
    public static class WordCount implements Serializable {
        private static final long serialVersionUID = 1L;
        public String word;
        public long count;

        public WordCount(final Word word, final long count) {
            this.word = word.getWord();
            this.count = count;
        }

        public WordCount() {} // for kryo

        static final Integer one = new Integer(1);

        @MessageKey
        public String getKey() {
            return word;
        }
    }

    public static class WordProducer implements Adaptor {
        private final AtomicBoolean isRunning = new AtomicBoolean(false);
        private Dispatcher dispatcher = null;
        private final AtomicBoolean done = new AtomicBoolean(false);
        public boolean onePass = true;
        public static CountDownLatch latch = new CountDownLatch(0);
        KeyExtractor ke = new KeyExtractor();
        int numDispatched = 0;

        private static String[] strings;

        static {
            try {
                setupStream();
            } catch (final Throwable e) {
                LOGGER.error("Failed to load source data", e);
            }
        }

        @Override
        public void setDispatcher(final Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Override
        public void start() {
            try {
                latch.await();
            } catch (final InterruptedException ie) {
                throw new RuntimeException(ie);
            }
            isRunning.set(true);
            while (isRunning.get() && !done.get()) {
                // obtain data from an external source
                final String wordString = getNextWordFromSoucre();
                try {
                    dispatcher.dispatch(ke.extract(new Word(wordString)));
                    numDispatched++;
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                    LOGGER.error("Failed to dispatch", e);
                }
            }
        }

        @Override
        public void stop() {
            isRunning.set(false);
        }

        private int curCount = 0;

        private String getNextWordFromSoucre() {
            final String ret = strings[curCount++];
            if (curCount >= strings.length) {
                if (onePass)
                    done.set(true);
                curCount = 0;
            }

            return ret;
        }

        private synchronized static void setupStream() throws IOException {
            if (strings == null) {
                final InputStream is = new GZIPInputStream(
                        new BufferedInputStream(WordProducer.class.getClassLoader().getResourceAsStream(wordResource)));
                final StringWriter writer = new StringWriter();
                IOUtils.copy(is, writer);
                strings = writer.toString().split("\\s+");
            }
        }
    }

    @Mp
    public static class WordCounter implements Cloneable {
        long counter = 0;
        String wordText;

        @Activation
        public void initMe(final String key) {
            this.wordText = key;
        }

        @MessageHandler
        public WordCount handle(final Word word) {
            return new WordCount(word, counter++);
        }

        @Override
        public WordCounter clone() throws CloneNotSupportedException {
            return (WordCounter) super.clone();
        }
    }

    public static class Rank {
        public final String work;
        public final Long rank;

        public Rank(final String work, final long rank) {
            this.work = work;
            this.rank = rank;
        }
    }

    @Mp
    public static class WordRank implements Cloneable {
        final public Map<String, Long> countMap = new ConcurrentHashMap<String, Long>();

        @MessageHandler
        public void handle(final WordCount wordCount) {
            countMap.put(wordCount.word, wordCount.count);
        }

        @Override
        public WordRank clone() throws CloneNotSupportedException {
            return (WordRank) super.clone();
        }

        public List<Rank> getPairs() {
            final List<Rank> ret = new ArrayList<>(countMap.size() + 10);
            for (final Map.Entry<String, Long> cur : countMap.entrySet())
                ret.add(new Rank(cur.getKey(), cur.getValue()));
            Collections.sort(ret, new Comparator<Rank>() {
                @Override
                public int compare(final Rank o1, final Rank o2) {
                    return o2.rank.compareTo(o1.rank);
                }

            });
            return ret;
        }
    }

    // ========================================================================

    Set<String> finalResults = new HashSet<String>();
    {
        finalResults.addAll(Arrays.asList("the", "and", "of", "to", "And", "in", "that", "he", "shall", "unto", "I"));
    }

    @Test
    public void testWordCount() throws Throwable {

        final String[] ctxs = {
                "classpath:/td/node.xml",
                "classpath:/td/transport-bq.xml",
                "classpath:/word-count/adaptor-kjv.xml",
                "classpath:/word-count/mp-word-count.xml",
                // "classpath:/word-count/mp-word-rank.xml",
        };

        WordProducer.latch = new CountDownLatch(1); // need to make it wait.
        final WordProducer adaptor;
        final MetricGetters stats;

        try (@SuppressWarnings("resource")
        final SystemPropertyManager props = new SystemPropertyManager()
                .set("routing-strategy", "net.dempsy.router.simple")
                .set("container-type", "net.dempsy.container.locking");
                final ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(ctxs);
                @SuppressWarnings("resource")
        final NodeManager manager = new NodeManager().node(ctx.getBean(Node.class))
                .collaborator(new LocalClusterSessionFactory().createSession()).start();) {
            assertTrue(poll(o -> manager.isReady()));

            WordProducer.latch.countDown();

            adaptor = ctx.getBean(WordProducer.class);
            stats = ctx.getBean(MetricGetters.class);

            assertTrue(poll(o -> adaptor.done.get()));
            assertTrue(poll(o -> adaptor.numDispatched == stats.getProcessedMessageCount()));

        }

        assertTrue(poll(o -> !adaptor.isRunning.get()));
    }
}
