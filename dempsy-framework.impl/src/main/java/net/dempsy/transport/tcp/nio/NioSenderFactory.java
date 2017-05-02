package net.dempsy.transport.tcp.nio;

import static net.dempsy.util.Functional.chain;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.dempsy.Infrastructure;
import net.dempsy.Manager;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.serialization.Serializer;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.NodeAddress;
import net.dempsy.transport.SenderFactory;
import net.dempsy.transport.tcp.nio.NioUtils.ReturnableBufferOutput;
import net.dempsy.util.SpinLock;

public class NioSenderFactory implements SenderFactory {
    private final static Logger LOGGER = LoggerFactory.getLogger(NioSenderFactory.class);

    public static final String CONFIG_KEY_SENDER_THREADS = "send_threads";
    public static final String DEFAULT_SENDER_THREADS = "1";

    public static final String CONFIG_KEY_SENDER_BLOCKING = "send_blocking";
    public static final String DEFAULT_SENDER_BLOCKING = "true";

    public static final String CONFIG_KEY_SENDER_MAX_QUEUED = "send_max_queued";
    public static final String DEFAULT_SENDER_MAX_QUEUED = "1000";

    private final Manager<Serializer> serializerManager = new Manager<Serializer>(Serializer.class);
    private final ConcurrentHashMap<NioAddress, NioSender> senders = new ConcurrentHashMap<>();

    private NodeStatsCollector statsCollector;
    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    String nodeId;

    int maxNumberOfQueuedOutgoing;
    boolean blocking;

    Sending[] sendings;

    @Override
    public void close() {
        final List<NioSender> snapshot;
        synchronized (this) {
            isRunning.set(false);
            snapshot = new ArrayList<>(senders.values());
        }
        snapshot.forEach(s -> s.stop());

        // we SHOULD be all done.
        final boolean recurse;
        synchronized (this) {
            recurse = senders.size() > 0;
        }
        if (recurse)
            close();

    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public NioSender getSender(final NodeAddress destination) throws MessageTransportException {
        final NioSender ret = senders.get(destination);
        if (ret == null) {
            synchronized (this) {
                if (isRunning.get()) {
                    final NioAddress tcpaddr = (NioAddress) destination;
                    return senders.computeIfAbsent(tcpaddr, a -> new NioSender(a, this, statsCollector, serializerManager, blocking, queue));
                } else {
                    throw new IllegalStateException(NioSender.class.getSimpleName() + " is stopped.");
                }
            }
        }
        return ret;
    }

    @Override
    public void start(final Infrastructure infra) {
        this.statsCollector = infra.getNodeStatsCollector();
        this.nodeId = infra.getNodeId();

        final int numSenderThreads = Integer
                .parseInt(infra.getConfigValue(NioSender.class, CONFIG_KEY_SENDER_THREADS, DEFAULT_SENDER_THREADS));

        maxNumberOfQueuedOutgoing = Integer.parseInt(infra.getConfigValue(NioSender.class, CONFIG_KEY_SENDER_MAX_QUEUED, DEFAULT_SENDER_MAX_QUEUED));

        blocking = Boolean.parseBoolean(infra.getConfigValue(NioSender.class, CONFIG_KEY_SENDER_BLOCKING, DEFAULT_SENDER_BLOCKING));

        sendings = new Sending[numSenderThreads];

        // now start the sending threads.
        for (int i = 0; i < sendings.length; i++)
            chain(new Thread(sendings[i], "nio-sender-" + i + "-" + nodeId), t -> t.start());

    }

    void imDone(final NioAddress tcp) {
        senders.remove(tcp);
    }

    private static class SenderHolder {
        public final NioSender sender;
        public final LinkedList<ReturnableBufferOutput> serializedMessages = new LinkedList<>();
        public ToWrite previousToWrite;

        SenderHolder(final NioSender sender) {
            this.sender = sender;
        }
    }

    private static class ToSerialize {
        public final SenderHolder sender;
        private final BlockingQueue<Object> messages;

        ToSerialize(final SenderHolder sender) {
            this.sender = sender;
            this.messages = this.sender.sender.messages;
        }

        boolean hasData() {
            return messages.peek() != null;
        }
    }

    private static class ToWrite {
        public final ReturnableBufferOutput ob;
        public final ToWrite leftover;

        ToWrite(final ReturnableBufferOutput ob, final ToWrite leftover) {
            this.ob = ob;
            ob.flop();
            this.leftover = leftover;
        }

        ToWrite write(final SocketChannel channel) throws IOException {
            final ByteBuffer cbb = ob.bb;
            if (leftover != null) {
                final ByteBuffer lobb = leftover.ob.bb;
                final ByteBuffer[] bbs = new ByteBuffer[2];
                bbs[0] = lobb;
                bbs[1] = cbb;
                channel.write(bbs);
                if (lobb.remaining() == 0) { // finished writing leftovers
                    leftover.ob.close(); // return it to the pool after clearing
                    if (cbb.remaining() == 0) {
                        // all written
                        ob.close();
                        return null;
                    } else {
                        // we finished the leftovers but we didn't finish the current
                        return new ToWrite(ob, null);
                    }
                } else { // there's some leftover from the leftovers
                    return this;
                }
            } else { // otherwise we had no leftover anyway
                channel.write(cbb);
                if (cbb.remaining() == 0) {
                    // it's finished.
                    ob.close();
                    return null;
                } else {
                    return this;
                }
            }
        }

    }

    public static class Sending implements Runnable {
        final AtomicBoolean isRunning;
        final boolean batch;
        final Selector selector;
        final String nodeId;

        Sending(final AtomicBoolean isRunning, final boolean batch, final String nodeId) throws IOException {
            this.batch = batch;
            this.isRunning = isRunning;
            this.nodeId = nodeId;
            this.selector = Selector.open();
        }

        @Override
        public void run() {
            int numNothing = 0;
            while (isRunning.get()) {
                try {
                    // blocking causes attempts to register to block creating a potential deadlock
                    final int numSelected = selector.selectNow();

                    // manage
                    if (numSelected == 0) {
                        // nothing ready ... might as well spend some time serializing messages.
                        final Set<SelectionKey> keys = selector.keys();
                        if (keys != null && keys.size() > 0) {
                            numNothing = 0; // reset the yield count since we have something to do
                            serializeNext(false, keys);
                        } else {
                            numNothing++;
                            if (numNothing > 1000)
                                Thread.sleep(1);
                            else
                                Thread.yield();
                            continue;
                        }
                    } else
                        numNothing = 0; // reset the yield count since we have something to do

                    final Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                    while (keys.hasNext()) {
                        final SelectionKey key = keys.next();

                        if (!key.isValid())
                            continue;

                        if (key.isWritable()) {
                            final SenderHolder sh = (SenderHolder) key.attachment();
                            final ToWrite tw;
                            final ToWrite tw1 = getDataToSend(sh);
                            if (tw1 == null) { // we need to discard the blocking queue and unregister the socket.
                                try (SpinLock.Guard g = sh.sender.mine.guardedWait()) {
                                    // double checked locking ... ToWrite has all final attributes.
                                    final ToWrite tw2 = getDataToSend(sh);
                                    if (tw2 == null) {
                                        sh.sender.clearThreadOwner(this);
                                        key.cancel();
                                    }
                                    tw = tw2;
                                }
                            } else
                                tw = tw1;

                            if (tw != null) { // we have something to write.
                                final SocketChannel channel = (SocketChannel) key.channel();
                                sh.previousToWrite = tw.write(channel);
                            }
                        }
                    }

                } catch (final IOException ioe) {
                    LOGGER.error(nodeId + " sender failed", ioe);
                } catch (final InterruptedException ie) {
                    if (isRunning.get())
                        LOGGER.error(nodeId + " sender interrupted", ie);
                }
            }
        }

        public void register(final NioSender sender) throws ClosedChannelException {
            final SocketChannel ch = sender.channel;
            ch.register(selector, SelectionKey.OP_WRITE, new SenderHolder(sender));
        }
    }

    private static ToWrite getDataToSend(final SenderHolder sender) throws IOException {
        if (sender.previousToWrite != null) {
            if (sender.previousToWrite.leftover != null) {
                return sender.previousToWrite;
            }
        }

        final LinkedList<ReturnableBufferOutput> messages = sender.serializedMessages;
        final ReturnableBufferOutput ob = messages.getFirst() != null ? messages.removeFirst() : null;
        if (ob != null)
            return new ToWrite(ob, sender.previousToWrite);
        else // serialize data
            return serialize(true, new ToSerialize(sender));
    }

    private static void serialize(final Serializer ser, final Object obj, final ReturnableBufferOutput ob) throws IOException {
        // we're going to assume the object's size will fit in a short.
        ob.writeShort((short) -1); // put a blank and push the buffer position ahead 2 bytes.
        final int pos = ob.getPosition(); // pos is the position AFTER the short was written
        ser.serialize(obj, ob);
        final int size = ob.getPosition() - pos;
        if (size > Short.MAX_VALUE) { // we need to cram an int in after the short.
            // make sure the buffer is big enough
            ob.writeInt(-1); // push 4 more bytes in.
            final byte[] buf = ob.getBuffer();
            System.arraycopy(buf, pos, buf, pos + 4, size); // slide the message right 4 bytes
            ob.setPosition(pos);
            ob.writeInt(size);
            ob.setPosition(ob.getPosition() + size);
        } else { // we need to write the short at the original position
            ob.setPosition(pos - 2);
            ob.writeShort((short) size);
            ob.setPosition(ob.getPosition() + size);
        }
    }

    private static ToWrite serialize(final boolean returnValue, final ToSerialize toSerialize) throws IOException {
        if (toSerialize != null) {
            final SenderHolder senderHolder = toSerialize.sender;
            final Serializer ser = senderHolder.sender.serializer;
            final BlockingQueue<Object> messages = toSerialize.messages;
            Object next;
            final ReturnableBufferOutput ob = NioUtils.get();
            boolean didSomething = false;
            while ((next = messages.poll()) != null) {
                didSomething = true;
                serialize(ser, next, ob);
            }

            if (!didSomething) {
                LOGGER.warn("Could have sworn I had something to serialize but it seems to have disappeared. This shouldn't be possible.");
                return null;
            }

            // we either return the value or we add it to the
            if (returnValue)
                return new ToWrite(ob, null);
            else
                senderHolder.serializedMessages.add(ob);
        }
        return null;
    }

    private static ToWrite serializeNext(final boolean returnValue, final Set<SelectionKey> keys) throws IOException {
        // pick a message to serialize.
        final ToSerialize toSerialize = keys.stream()
                .map(k -> (SenderHolder) k.attachment())
                .map(sh -> new ToSerialize(sh))
                .filter(ts -> ts.hasData())
                .findFirst().orElse(null);

        return serialize(returnValue, toSerialize);
    }
}
