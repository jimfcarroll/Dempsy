package net.dempsy.transport.tcp.netty;

import java.net.InetAddress;

import net.dempsy.transport.tcp.TcpAddress;

public class NettyAddress extends TcpAddress {
    private static final long serialVersionUID = 1L;

    public NettyAddress(final InetAddress inetAddress, final int port, final String serializerId, final int recvBufferSize) {
        super(inetAddress, port, serializerId, recvBufferSize);
    }

    @SuppressWarnings("unused")
    private NettyAddress() {}
}
