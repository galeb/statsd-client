package io.galeb.statsd;

@SuppressWarnings("unused")
public interface StatsDClient {

    void stop();

    void count(String aspect, int delta, String... tags);

    void incrementCounter(String aspect, String... tags);

    void increment(String aspect, String... tags);

    void decrementCounter(String aspect, String... tags);

    void decrement(String aspect, String... tags);

    void recordGaugeValue(String aspect, double value, String... tags);

    void gauge(String aspect, double value, String... tags);

    void recordGaugeValue(String aspect, int value, String... tags);

    void gauge(String aspect, int value, String... tags);

    void recordExecutionTime(String aspect, long timeInMs, String... tags);

    void time(String aspect, long value, String... tags);

    void recordHistogramValue(String aspect, double value, String... tags);

    void histogram(String aspect, double value, String... tags);

    void recordHistogramValue(String aspect, int value, String... tags);

    void histogram(String aspect, int value, String... tags);

}
