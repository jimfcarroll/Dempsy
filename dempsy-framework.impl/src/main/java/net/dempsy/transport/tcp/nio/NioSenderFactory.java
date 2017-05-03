package net.dempsy.transport.tcp.nio;

import static net.dempsy.util.Functional.chain;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashSet;
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
import net.dempsy.transport.tcp.nio.NioSender.StopMessage;
import net.dempsy.transport.tcp.nio.NioUtils.ReturnableBufferOutput;
import net.dempsy.util.StupidHashMap;

public class NioSenderFactory implements SenderFactory {
    private final static Logger LOGGER = LoggerFactory.getLogger(NioSenderFactory.class);

    public static final String CONFIG_KEY_SENDER_THREADS = "send_threads";
    public static final String DEFAULT_SENDER_THREADS = "1";

    public static final String CONFIG_KEY_SENDER_BLOCKING = "send_blocking";
    public static final String DEFAULT_SENDER_BLOCKING = "true";

    public static final String CONFIG_KEY_SENDER_MAX_QUEUED = "send_max_queued";
    public static final String DEFAULT_SENDER_MAX_QUEUED = "1000";

    private final ConcurrentHashMap<NioAddress, NioSender> senders = new ConcurrentHashMap<>();

    final StupidHashMap<NioSender, NioSender> working = new StupidHashMap<>();

    // =======================================
    // Read from NioSender
    final Manager<Serializer> serializerManager = new Manager<Serializer>(Serializer.class);
    final AtomicBoolean isRunning = new AtomicBoolean(true);
    NodeStatsCollector statsCollector;
    String nodeId;
    int maxNumberOfQueuedOutgoing;
    boolean blocking;
    // =======================================

    Sending[] sendings;

    @Override
    public void close() {
        LOGGER.trace(nodeId + " stopping " + NioSenderFactory.class.getSimpleName());
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
        final NioAddress tcpaddr = (NioAddress) destination;
        final NioSender ret;
        if (isRunning.get()) {
            ret = senders.computeIfAbsent(tcpaddr, a -> new NioSender(a, this));
        } else
            throw new MessageTransportException(nodeId + " sender had getSender called while stopped.");

        try {
            ret.connect();
        } catch (final IOException e) {
            throw new MessageTransportException(nodeId + " sender failed to connect to " + destination.getGuid(), e);
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
            chain(new Thread(sendings[i] = new Sending(isRunning, nodeId, working, statsCollector), "nio-sender-" + i + "-" + nodeId),
                    t -> t.start());

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
            final Object peek = messages.peek();
            if (peek instanceof StopMessage) {
                NioUtils.closeQueitly(sender.sender.channel, LOGGER, "Failed to close channel.");
                return false;
            }
            return messages.peek() != null;
        }
    }

    private static class ToWrite {
        public final ReturnableBufferOutput ob;

        ToWrite(final ReturnableBufferOutput ob) {
            this.ob = ob;
            ob.flop();
        }

        ToWrite write(final SocketChannel channel, final NodeStatsCollector statsCollector) throws IOException {
            final ByteBuffer cbb = ob.getBb();
            final int numBytes = channel.write(cbb);
            System.out.println("2) Sent " + numBytes);

            if (cbb.remaining() == 0) {
                // it's finished.
                for (int i = 0; i < ob.numMessages; i++)
                    statsCollector.messageSent(null);

                ob.close();
                return null;
            } else {
                return this;
            }
        }
    }

    public static class Sending implements Runnable {
        final AtomicBoolean isRunning;
        final Selector selector;
        final String nodeId;
        final StupidHashMap<NioSender, NioSender> working;
        final NodeStatsCollector statsCollector;

        Sending(final AtomicBoolean isRunning, final String nodeId, final StupidHashMap<NioSender, NioSender> working,
                final NodeStatsCollector statsCollector)
                throws MessageTransportException {
            this.isRunning = isRunning;
            this.nodeId = nodeId;
            this.working = working;
            this.statsCollector = statsCollector;
            try {
                this.selector = Selector.open();
            } catch (final IOException e) {
                throw new MessageTransportException(e);
            }
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
                        // nothing ready ... might as well spend some time serializing messages
                        final Set<SelectionKey> keys = selector.keys();
                        if (keys != null && keys.size() > 0) {
                            numNothing = 0; // reset the yield count since we have something to do
                            serializeNext(false, keys, statsCollector);
                        } else { // nothing to serialize, do we have any new senders that need handling?
                            boolean didSomething = false;
                            final Set<NioSender> curSenders = working.keySet();
                            final Set<NioSender> newSenders = new HashSet<>();

                            try { // if we fail here we need to put the senders back or we'll loose them forever.

                                // move any NioSenders with data from working and onto newSenders
                                curSenders.stream()
                                        .filter(s -> s.messages.peek() != null)
                                        .forEach(s -> {
                                            final NioSender cur = working.remove(s);
                                            // removing them means putting them on the newSenders set so we can track them
                                            if (cur != null)
                                                newSenders.add(cur);
                                        });

                                // newSenders are now mine since they've been removed from working.

                                // go through each new sender ...
                                for (final Iterator<NioSender> iter = newSenders.iterator(); iter.hasNext();) {
                                    final NioSender cur = iter.next();

                                    // ... if the new sender has messages ...
                                    if (cur.messages.peek() != null) {
                                        // ... regsiter the channel for writing
                                        final SocketChannel ch = cur.channel;
                                        ch.register(selector, SelectionKey.OP_WRITE, new SenderHolder(cur));
                                        iter.remove();
                                        didSomething = true; // we did something.
                                    }
                                }
                            } finally {
                                // any still on toWork need to be returned to working
                                newSenders.forEach(s -> working.put(s, s));
                            }

                            if (!didSomething) { // if we didn't do anything then sleep/yield based on how long we've been bord.
                                numNothing++;
                                if (numNothing > 1000) {
                                    try {
                                        Thread.sleep(1);
                                    } catch (final InterruptedException ie) {
                                        if (isRunning.get())
                                            LOGGER.error(nodeId + " sender interrupted", ie);
                                    }
                                } else
                                    Thread.yield();
                            } else // otherwise we DID do something
                                numNothing = 0;

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
                            final ToWrite tw = getDataToSend(sh, statsCollector);
                            if (tw == null) { // we need to discard the blocking queue and unregister the socket.
                                working.putIfAbsent(sh.sender, sh.sender);
                                key.cancel();
                            } else { // we have something to write.
                                final SocketChannel channel = (SocketChannel) key.channel();
                                sh.previousToWrite = tw.write(channel, statsCollector);
                            }
                        }
                    }
                } catch (final IOException ioe) {
                    LOGGER.error(nodeId + " sender failed", ioe);
                } finally {
                    // LOGGER.trace("looping sending thread:" + numNothing);
                }
            }
        }

    }

    private static ToWrite getDataToSend(final SenderHolder sender, final NodeStatsCollector statsCollector) throws IOException {
        if (sender.previousToWrite != null) {
            return sender.previousToWrite;
        }

        final LinkedList<ReturnableBufferOutput> messages = sender.serializedMessages;
        final ReturnableBufferOutput ob = messages.peek() != null ? messages.removeFirst() : null;
        if (ob != null)
            return new ToWrite(ob);
        else // serialize data
            return serialize(true, new ToSerialize(sender), statsCollector);
    }

    private static void serialize(final Serializer ser, final Object obj, final ReturnableBufferOutput ob) throws IOException {
        final int pos1 = ob.getPosition();
        ob.setPosition(pos1 + 4);
        final int pos2 = ob.getPosition();
        ser.serialize(obj, ob);
        final int pos3 = ob.getPosition();
        ob.numMessages++;
        final int size = ob.getPosition() - pos2;
        ob.setPosition(pos1);
        ob.writeInt(size);
        ob.setPosition(pos3);

        // // we're going to assume the object's size will fit in a short.
        // ob.writeShort((short) -1); // put a blank and push the buffer position ahead 2 bytes.
        // final int pos = ob.getPosition(); // pos is the position AFTER the short was written
        // ser.serialize(obj, ob);
        // ob.numMessages++;
        // final int size = ob.getPosition() - pos;
        // if (size > Short.MAX_VALUE) { // we need to cram an int in after the short.
        // // make sure the buffer is big enough
        // ob.writeInt(-1); // push 4 more bytes in.
        // final byte[] buf = ob.getBuffer();
        // System.arraycopy(buf, pos, buf, pos + 4, size); // slide the message right 4 bytes
        // ob.setPosition(pos);
        // ob.writeInt(size);
        // ob.setPosition(ob.getPosition() + size);
        // } else { // we need to write the short at the original position
        // ob.setPosition(pos - 2);
        // ob.writeShort((short) size);
        // ob.setPosition(ob.getPosition() + size);
        // }
    }

    private static ToWrite serialize(final boolean returnValue, final ToSerialize toSerialize, final NodeStatsCollector statsCollector)
            throws IOException {
        if (toSerialize != null) {
            final SenderHolder senderHolder = toSerialize.sender;
            final Serializer ser = senderHolder.sender.serializer;
            final BlockingQueue<Object> messages = toSerialize.messages;
            Object next;
            final ReturnableBufferOutput ob = NioUtils.get();
            while ((next = messages.poll()) != null) {
                if (next instanceof StopMessage) {
                    NioUtils.closeQueitly(toSerialize.sender.sender.channel, LOGGER, "Failed to close channel.");
                    while (messages.poll() != null)
                        statsCollector.messageNotSent();
                    for (int i = 0; i < ob.numMessages; i++)
                        statsCollector.messageNotSent();
                    return null;
                }
                serialize(ser, next, ob);
            }

            if (ob.numMessages == 0) {
                // no messages, we went on one more loop after emptying the queue and there's nothing here so we're going to cleanup.
                return null;
            }

            // we either return the value or we add it to the
            if (returnValue)
                return new ToWrite(ob);
            else
                senderHolder.serializedMessages.add(ob);

        }
        return null;
    }

    private static ToWrite serializeNext(final boolean returnValue, final Set<SelectionKey> keys, final NodeStatsCollector statsCollector)
            throws IOException {
        // pick a message to serialize.
        final ToSerialize toSerialize = keys.stream()
                .map(k -> (SenderHolder) k.attachment())
                .map(sh -> new ToSerialize(sh))
                .filter(ts -> ts.hasData())
                .findFirst().orElse(null);

        return serialize(returnValue, toSerialize, statsCollector);
    }
}
