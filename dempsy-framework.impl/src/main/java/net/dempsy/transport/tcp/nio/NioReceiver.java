package net.dempsy.transport.tcp.nio;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class NioReceiver<T> {

    public static class Binding {
        public final Selector selector;
        public final ServerSocketChannel serverChannel;
        public final InetSocketAddress bound;

        public Binding(final InetAddress addr, final int port) throws IOException {
            final int lport = port < 0 ? 0 : port;
            selector = Selector.open();

            serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(false);

            bound = new InetSocketAddress(addr, lport);
            serverChannel.socket().bind(bound);
        }
    }

    public static class Client<T> {

    }

    public static class Reader<T> implements Runnable {

        public AtomicReference<SocketChannel> landing = new AtomicReference<SocketChannel>(null);
        public final Selector selector;
        public final AtomicBoolean isRunning;

        public Reader(final AtomicBoolean isRunning) throws IOException {
            selector = Selector.open();
            this.isRunning = isRunning;
        }

        @Override
        public void run() {

            while (isRunning.get()) {
                try {
                    selector.select();

                    final Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                    boolean hadAReadKey = false;
                    while (keys.hasNext()) {
                        final SelectionKey key = keys.next();

                        keys.remove();

                        if (!key.isValid())
                            continue;

                        if (key.isReadable()) {
                            hadAReadKey = true;
                            read(key);
                        }
                    }

                    // if we processed no keys then maybe we have a new client passed over to us.
                    if (!hadAReadKey) {
                        final SocketChannel newClient = landing.getAndSet(null);
                        if (newClient != null) {
                            // we have a new client
                            newClient.configureBlocking(false);
                            final Socket socket = newClient.socket();
                            final SocketAddress remote = socket.getRemoteSocketAddress();
                            newClient.register(selector, SelectionKey.OP_READ, new Client<T>());
                        }
                    }
                } catch (final IOException ioe) {
                    // TODO: something
                }
            }
        }

        public synchronized void newClient(final SocketChannel newClient) {
            // attempt to set the landing as long as it's null
            while (landing.compareAndSet(null, newClient))
                Thread.yield();

            // wait until the Reader runnable takes it.
            while (landing.get() != null) {
                selector.wakeup();
                Thread.yield();
            }
        }
    }

    public static class Acceptor implements Runnable {

        final Binding binding;
        final AtomicBoolean isRunning;

        Acceptor(final Binding binding, final AtomicBoolean isRunning) {
            this.binding = binding;
            this.isRunning = isRunning;
        }

        @Override
        public void run() {
            final Selector selector = binding.selector;
            final ServerSocketChannel serverChannel = binding.serverChannel;

            try {
                serverChannel.register(selector, SelectionKey.OP_ACCEPT);

                while (isRunning.get()) {
                    selector.select();

                    final Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                    while (keys.hasNext()) {
                        final SelectionKey key = keys.next();

                        keys.remove();

                        if (!key.isValid())
                            continue;

                        if (key.isAcceptable()) {
                            accept(key);
                        }
                    }
                }
            } catch (final IOException ioe) {
                // TODO: not this
            }
        }

        private void accept(final SelectionKey key) {
            // TODO: select a Reader and pass the new client's socket channel using Reader.newClient
        }
    }
}
