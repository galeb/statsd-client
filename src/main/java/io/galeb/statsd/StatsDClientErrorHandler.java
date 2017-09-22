package io.galeb.statsd;

public interface StatsDClientErrorHandler {

    void handle(Exception exception);

}
