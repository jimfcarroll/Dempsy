package net.dempsy.transport.tcp.netty;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.MessageToMessageEncoder;
import net.dempsy.DempsyException;
import net.dempsy.Manager;
import net.dempsy.monitoring.NodeStatsCollector;
import net.dempsy.serialization.Serializer;
import net.dempsy.transport.MessageTransportException;
import net.dempsy.transport.Sender;
import net.dempsy.transport.tcp.TcpAddress;
import net.dempsy.util.io.MessageBufferOutput;

public final class NettySender implements Sender {
    private final static Logger LOGGER = LoggerFactory.getLogger(NettySender.class);

    private final NodeStatsCollector statsCollector;
    private final TcpAddress addr;
    private final Serializer serializer;
    private final NettySenderFactory owner;
    private final AtomicReference<Internal> connection = new AtomicReference<>(null);
    private boolean isRunning = true;

    public NettySender(final TcpAddress addr, final NettySenderFactory parent, final NodeStatsCollector statsCollector) {
        this.addr = addr;
        serializer = new Manager<Serializer>(Serializer.class).getAssociatedInstance(addr.serializerId);
        this.statsCollector = statsCollector;
        this.owner = parent;
        reset();
    }

    @Override
    public void send(final Object message) throws MessageTransportException {
        try {
            final Internal cur = connection.get();
            if (cur != null) {
                connection.get().ch.writeAndFlush(message).sync();
            }
        } catch (final InterruptedException e) {
            throw new MessageTransportException(e);
        }
    }

    @Override
    public synchronized void stop() {
        isRunning = false;
        reset();
        owner.imDone(addr);
    }

    private void reset() {
        final Internal previous = connection.getAndSet(isRunning ? new Internal() : null);

        if (previous != null) {
            do {
                try {
                    previous.ch.close().sync();
                } catch (final InterruptedException e) {
                    synchronized (this) {
                        if (isRunning)
                            LOGGER.warn("Interrupted during close.");
                    }
                }
            } while (previous.ch.isOpen() && isRunning);
        }
    }

    private class Internal {
        Channel ch = null;

        Internal() {
            reset();
        }

        void reset() {
            final EventLoopGroup group = new NioEventLoopGroup();
            try {
                final Bootstrap b = new Bootstrap();
                b.group(group)
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {

                            @Override
                            protected void initChannel(final SocketChannel ch) throws Exception {
                                final ChannelPipeline pipeline = ch.pipeline();
                                pipeline.addLast(new MessageToMessageEncoder<Object>() {

                                    @Override
                                    protected void encode(final ChannelHandlerContext ctx, final Object msg, final List<Object> out)
                                            throws Exception {
                                        // TODO: pool these
                                        final MessageBufferOutput o = new MessageBufferOutput();
                                        serializer.serialize(msg, o);
                                        ByteBuf preamble;
                                        if (o.getPosition() > Short.MAX_VALUE) {
                                            try (final MessageBufferOutput preamblembo = new MessageBufferOutput(6);) {
                                                preamblembo.writeShort((short) -1);
                                                preamblembo.writeInt(o.getPosition());
                                                preamble = Unpooled.wrappedBuffer(preamblembo.getBuffer());
                                            }
                                        } else {
                                            try (final MessageBufferOutput preamblembo = new MessageBufferOutput(2);) {
                                                preamblembo.writeShort((short) o.getPosition());
                                                preamble = Unpooled.wrappedBuffer(preamblembo.getBuffer());
                                            }
                                        }
                                        out.add(preamble);
                                        out.add(Unpooled.wrappedBuffer(o.getBuffer(), 0, o.getPosition()));
                                        if (statsCollector != null)
                                            statsCollector.messageSent(msg);
                                    }

                                    @Override
                                    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
                                        LOGGER.error("Failed writing to {}", addr, cause);
                                        // TODO: pass the failed message over for another try.
                                        NettySender.this.reset();
                                    }
                                });
                            }
                        }

                );

                // Start the connection attempt.
                ch = b.connect(addr.inetAddress, addr.port).sync().channel();

            } catch (final InterruptedException ie) {
                throw new DempsyException(ie);
            }
        }

    }

}