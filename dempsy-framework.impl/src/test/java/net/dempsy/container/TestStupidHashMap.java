package net.dempsy.container;

import static net.dempsy.util.Functional.chain;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import net.dempsy.container.nonlocking.StupidHashMap;

@RunWith(Parameterized.class)
public class TestStupidHashMap {

    public static final int NUMWRITERS = 8;
    public static final int NUMREADERS = 2;
    public static final int NUMWRITES = 500000;

    Map<Integer, Integer> it;

    @Parameters
    public static Object[][] params() {
        return new Object[][] {
                // { (Supplier<Map<Integer, Integer>>) () -> new ConcurrentHashMap<Integer, Integer>(2048) },
                { (Supplier<Map<Integer, Integer>>) () -> new StupidHashMap<Integer, Integer>(1) },
                { (Supplier<Map<Integer, Integer>>) () -> new StupidHashMap<Integer, Integer>() },
        };
    }

    public TestStupidHashMap(final Supplier<Map<Integer, Integer>> supplier) {
        this.it = supplier.get();
    }

    @Test
    public void testPouning() throws InterruptedException {

        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean failedWrites = new AtomicBoolean(false);
        final AtomicBoolean failedReads = new AtomicBoolean(false);
        final AtomicBoolean failedChecks = new AtomicBoolean(false);

        try {
            final Integer[] values = createUniqeSet(1000);

            assertEquals(1000, values.length);

            // preload the map
            for (int ii = 0; ii < values.length; ii++) {
                final Integer kv = values[ii];
                final Integer already = it.putIfAbsent(kv, kv);
                assertNull(already);
            }

            assertEquals(values.length, it.size());

            new Thread(() -> {
                while (!done.get()) {
                    if (it.size() != values.length)
                        failedChecks.set(true);
                }
            });

            final List<Thread> writers = IntStream.range(0, NUMWRITERS).mapToObj(i -> new Thread(() -> {
                for (int j = 0; j < NUMWRITES;) {
                    for (int ii = 0; ii < values.length; ii++) {
                        final Integer kv = values[ii];
                        if (it.putIfAbsent(kv, kv) == null) {
                            failedWrites.set(true);
                        }
                        j++;
                    }
                }
            }, "Writer-" + i)).map(t -> {
                t.start();
                return t;
            }).collect(Collectors.toList());

            IntStream.range(0, NUMREADERS).forEach(i -> new Thread(() -> {
                while (!done.get()) {
                    for (int ii = 0; ii < values.length; ii++) {
                        final Integer k = values[ii];
                        final Integer v = it.get(k);
                        if (v != null) {
                            if (k.intValue() != v.intValue())
                                failedReads.set(true);
                        }
                    }
                }
            }, "Reader-" + i).start());

            writers.forEach(t -> {
                try {
                    t.join(20000);
                    if (t.isAlive())
                        throw new IllegalStateException("Failed on writes.");
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

        } finally {
            done.set(true);
        }

        assertFalse(failedWrites.get());
        assertFalse(failedReads.get());
        assertFalse(failedChecks.get());

    }

    @Test
    public void testUniqueOwnershipSimple() throws InterruptedException {

        final AtomicBoolean done = new AtomicBoolean(false);
        final AtomicBoolean gotWhenIOwnSlot = new AtomicBoolean(false);
        final AtomicBoolean gotAnother = new AtomicBoolean(false);
        final AtomicBoolean removedAnother = new AtomicBoolean(false);

        try {
            final Integer[] values = createUniqeSet(1000);

            // preload the map
            for (int ii = 0; ii < values.length; ii++) {
                final Integer kv = values[ii];
                final Integer already = it.putIfAbsent(kv, kv);
                assertNull(already);
            }

            final Integer toWorkWith = values[500];

            it.remove(toWorkWith); // initialize it as owned by the putter thread.

            final List<Thread> workers = IntStream.range(0, NUMWRITERS).mapToObj(i -> chain(new Thread(() -> {
                boolean iOwnPositionInMap = false;
                final Integer value = new Integer(i);
                int numWrites = 0;
                while (!done.get() && numWrites < NUMWRITES) {
                    if (it.putIfAbsent(toWorkWith, value) == null) {
                        // it went in. I better not already own that position.
                        if (iOwnPositionInMap)
                            gotWhenIOwnSlot.set(true);;
                        iOwnPositionInMap = true; // it's my position in the map
                    }

                    if (iOwnPositionInMap) {
                        // I should be able to retrieve it.
                        final Integer shouldBeMyNumber = it.get(toWorkWith);
                        if (shouldBeMyNumber == null || !shouldBeMyNumber.equals(value))
                            gotAnother.set(true);

                        // now let's remove it.
                        final Integer shouldBeMyNumberAgain = it.remove(toWorkWith);
                        if (shouldBeMyNumberAgain == null || !shouldBeMyNumberAgain.equals(value))
                            removedAnother.set(true);
                        iOwnPositionInMap = false;
                    }
                    numWrites++;
                }
            }, "Putter"), t -> t.start())).collect(Collectors.toList());

            join(workers, 20000);
        } finally {
            done.set(true);
        }

        assertFalse("Got it in but already had it in.", gotWhenIOwnSlot.get());
        assertFalse("Retrieved an incorrect entry when owned", gotAnother.get());
        assertFalse("Removed an incorrect entry when owned", removedAnother.get());
    }

    @Test
    public void testPouningWithRemove() throws InterruptedException {

        final List<Long> numAdded = new ArrayList<Long>();
        final List<Long> numRemoved = new ArrayList<Long>();
        for (int loop = 0; loop < 3; loop++) {
            final AtomicBoolean done = new AtomicBoolean(false);

            final AtomicBoolean failedReads = new AtomicBoolean(false);

            try {
                // make unique values
                final Integer[] values = createUniqeSet(1000);

                assertEquals(1000, values.length);

                // preload the map
                for (int ii = 0; ii < values.length; ii++) {
                    final Integer kv = values[ii];
                    final Integer already = it.putIfAbsent(kv, kv);
                    if (loop == 0)
                        assertNull(already);
                }

                numAdded.add(new Long(values.length));

                if (loop == 0)
                    assertEquals(values.length, it.size());

                final List<Thread> writers = IntStream.range(0, NUMWRITERS).mapToObj(i -> startThread("Writer-" + i, () -> {
                    long c = 0;
                    for (int j = 0; j < NUMWRITES;) {
                        for (int ii = 0; ii < values.length; ii++) {
                            final Integer kv = values[ii];
                            j++;
                            if (it.putIfAbsent(kv, kv) == null)
                                c++; // this one got on
                        }
                    }
                    synchronized (numAdded) {
                        numAdded.add(new Long(c));
                    }
                })).collect(Collectors.toList());

                final List<Thread> removers = IntStream.range(0, NUMWRITERS).mapToObj(i -> startThread("Remover-" + i, () -> {
                    int c = 0;
                    while (!done.get()) {
                        for (int ii = 0; ii < values.length; ii++) {
                            final Integer k = values[ii];
                            final Integer v = it.remove(k);
                            if (v != null) {
                                c++;
                                if (k.intValue() != v.intValue())
                                    failedReads.set(true);
                            }
                        }
                    }
                    synchronized (numRemoved) {
                        numRemoved.add(new Long(c));
                    }
                })).collect(Collectors.toList());

                done.set(true); // allow threads to exit for join

                join(writers, 20000);

                join(removers, 20000);

            } finally {
                done.set(true);
            }

            assertEquals(it.size(), sum(numAdded) - sum(numRemoved));
            assertFalse(failedReads.get());
        }
    }

    @Test
    public void testUniqueOwnershipRotating() throws InterruptedException {

        for (int loop = 0; loop < 5; loop++) {
            System.out.println("Trying Yet Again");
            final AtomicBoolean done = new AtomicBoolean(false);
            final AtomicBoolean gotWhenIOwnedObject = new AtomicBoolean(false);
            final AtomicBoolean gotOne = new AtomicBoolean(false);
            final List<Integer> owners = new ArrayList<>();

            final Integer[] values = createUniqeSet(1000);

            // preload the map
            for (int ii = 0; ii < values.length; ii++) {
                final Integer kv = values[ii];
                final Integer already = it.putIfAbsent(kv, kv);
                assertNull(already);
            }

            final Integer toWorkWith = values[500];

            try {
                it.remove(toWorkWith); // initialize it as owned by the putter thread.

                final List<Thread> workers = IntStream.range(0, NUMWRITERS).mapToObj(i -> chain(new Thread(() -> {
                    boolean iOwnIt = i == 0;
                    final Integer value = new Integer(i);
                    try {
                        int numWrites = 0;
                        while (!done.get() && numWrites < NUMWRITES) {
                            if (iOwnIt) {
                                // gratuitous get ... it should be null since I'm holding it.
                                final Integer fromMap = it.get(toWorkWith);
                                if (fromMap != null) { // WHAT?
                                    System.err.println("I (" + value + ") owned it but one was on the map anyway.");
                                    gotOne.set(true);
                                    break;
                                }

                                final Integer alreadyThere = it.putIfAbsent(toWorkWith, value);
                                if (alreadyThere != null) {
                                    System.out.println("I (" + value + ")  owned it but there seemed to be one " + alreadyThere + "  there already.");
                                    gotWhenIOwnedObject.set(true);
                                    break;
                                }
                                iOwnIt = false;
                            } else { // maybe I can get it
                                final Integer fromMap = it.remove(toWorkWith);
                                if (fromMap != null) { // I got it
                                    iOwnIt = true;
                                }
                            }
                            numWrites++;
                        }
                    } finally {
                        if (iOwnIt) {
                            synchronized (owners) {
                                owners.add(value);
                            }
                        }
                    }
                }, "Putter"), t -> t.start())).collect(Collectors.toList());

                join(workers, 20000);
            } finally {
                done.set(true);
            }

            assertFalse("Got it back on an insert when I already had it.", gotWhenIOwnedObject.get());
            // if (gotWhenIOwnedObject.get()) {
            // System.out.println("assert");
            // }

            assertFalse("Retreived it when I already had it.", gotOne.get());
            // if (gotOne.get()) {
            // System.out.println("assert");
            // }

            if (it.get(toWorkWith) == null) {
                assertEquals("There seemed to be too many owners at the end " + owners, 1, owners.size());
                // if (owners.size() != 1) {
                // System.out.println("assert");
                // }
            } else {
                assertEquals("There seemed to be too many owners at the end " + owners, 0, owners.size());
                // if (owners.size() != 0) {
                // System.out.println("assert");
                // }
            }
        }
    }

    private static long sum(final List<Long> vals) {
        return vals.stream().reduce(new Long(0), (v1, v2) -> new Long(v1.longValue() + v2.longValue())).longValue();
    }

    private static Thread startThread(final String name, final Runnable r) {
        final Thread ret = new Thread(r, name);
        ret.start();
        return ret;
    }

    private static void join(final Thread t, final long timeout) {
        try {
            t.join(timeout);
            if (t.isAlive())
                throw new IllegalStateException("Failed on join.");
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void join(final List<Thread> ts, final long timeout) {
        ts.forEach(t -> join(t, timeout));
    }

    private static final Integer[] createUniqeSet(final int size) {
        final Random random = new Random();
        // make unique values
        final Set<Integer> svals = new HashSet<>();
        while (svals.size() < size) {
            svals.add(random.nextInt());
        }
        return svals.toArray(new Integer[svals.size()]);
    }
}
