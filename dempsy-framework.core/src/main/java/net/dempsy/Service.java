package net.dempsy;

public interface Service extends AutoCloseable {

    public void start(Infrastructure infra);

    public void stop();

    @Override
    public default void close() {
        stop();
    }
}
