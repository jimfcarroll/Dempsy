package net.dempsy.transport.tcp;

import net.dempsy.serialization.Serializer;
import net.dempsy.transport.Receiver;

public abstract class AbstractTcpReceiver<A extends TcpAddress, T extends AbstractTcpReceiver<A, ?>> implements Receiver {
    protected final Serializer serializer;

    protected int internalPort;
    protected boolean useLocalHost = false;
    protected TcpAddressResolver<A> resolver = a -> a;
    protected final String serId;

    public AbstractTcpReceiver(final Serializer serializer, final int port) {
        this.internalPort = port;
        this.serializer = serializer;
        this.serId = serializer.getClass().getPackage().getName();
    }

    public AbstractTcpReceiver(final Serializer serializer) {
        this(serializer, -1);
    }

    @SuppressWarnings("unchecked")
    public T setUseLocalHost(final boolean useLocalHost) {
        this.useLocalHost = useLocalHost;
        return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T setResolver(final TcpAddressResolver<A> resolver) {
        this.resolver = resolver;
        return (T) this;
    }

}
