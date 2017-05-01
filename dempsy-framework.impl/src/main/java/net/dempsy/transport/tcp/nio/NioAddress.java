package net.dempsy.transport.tcp.nio;

import java.net.InetAddress;

import net.dempsy.transport.tcp.TcpAddress;

public class NioAddress extends TcpAddress {
    private static final long serialVersionUID = 1L;

    public NioAddress(final InetAddress inetAddress, final int port, final String serializerId) {
        super(inetAddress, port, serializerId);
    }

}
