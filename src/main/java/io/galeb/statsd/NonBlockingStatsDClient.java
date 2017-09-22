package io.galeb.statsd;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.FatalExceptionHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.dsl.Disruptor;

public final class NonBlockingStatsDClient implements StatsDClient {

    private static final int PACKET_SIZE_BYTES = 1400;

    private static final StatsDClientErrorHandler NO_OP_HANDLER = e -> { /* No-op */ };

    private static final ThreadLocal<NumberFormat> NUMBER_FORMATTERS = ThreadLocal.withInitial(() -> {
        NumberFormat numberFormatter = NumberFormat.getInstance(Locale.US);
        numberFormatter.setGroupingUsed(false);
        numberFormatter.setMaximumFractionDigits(6);
        return numberFormatter;
    });

    private final static EventFactory<Event> FACTORY = Event::new;

    private static final EventTranslatorOneArg<Event, String> TRANSLATOR = (event, sequence, msg) -> event.setValue(msg);

    private final String prefix;
    private final DatagramChannel clientChannel;
    private final InetSocketAddress address;
    private final StatsDClientErrorHandler errorHandler;
    private final String constantTagsRendered;

    private final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
        final ThreadFactory delegate = Executors.defaultThreadFactory();

        @Override
        public Thread newThread(Runnable r) {
            Thread result = delegate.newThread(r);
            result.setName("StatsD-disruptor-" + result.getName());
            result.setDaemon(true);
            return result;
        }
    });

    private final Disruptor<Event> disruptor = new Disruptor<Event>(FACTORY, 16384, executor);

    public NonBlockingStatsDClient(String prefix, String hostname, int port) throws StatsDClientException {
        this(prefix, hostname, port, null, NO_OP_HANDLER);
    }

    public NonBlockingStatsDClient(String prefix, String hostname, int port, String[] constantTags) throws StatsDClientException {
        this(prefix, hostname, port, constantTags, NO_OP_HANDLER);
    }

    public NonBlockingStatsDClient(String prefix, String hostname, int port, String[] constantTags, StatsDClientErrorHandler errorHandler) throws StatsDClientException {
        this(prefix, hostname, port, constantTags, errorHandler, null);
    }

    @SuppressWarnings("unchecked")
    public NonBlockingStatsDClient(String prefix, String hostname, int port, String[] constantTags, StatsDClientErrorHandler errorHandler, EventHandler<Event> handler) throws StatsDClientException {
        if (prefix != null && prefix.length() > 0) {
            this.prefix = String.format("%s.", prefix);
        } else {
            this.prefix = "";
        }
        this.errorHandler = errorHandler;

        if (constantTags != null && constantTags.length > 0) {
            this.constantTagsRendered = tagString(constantTags, null);
        } else {
            this.constantTagsRendered = null;
        }

        try {
            this.clientChannel = DatagramChannel.open();
            this.address = new InetSocketAddress(hostname, port);
        } catch (Exception e) {
            throw new StatsDClientException("Failed to start StatsD client", e);
        }

        disruptor.handleExceptionsWith(new DisruptorExceptionHandler(this.errorHandler));
        disruptor.handleEventsWith(handler != null ? handler : new Handler());
        disruptor.start();
    }

    @Override
    public void stop() {
        try {
            disruptor.shutdown();
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            errorHandler.handle(e);
        } finally {
            if (clientChannel != null) {
                try {
                    clientChannel.close();
                } catch (IOException e) {
                    errorHandler.handle(e);
                }
            }
        }
    }

    static String tagString(final String[] tags, final String tagPrefix) {
        StringBuilder sb;
        if (tagPrefix != null) {
            if (tags == null || tags.length == 0) {
                return tagPrefix;
            }
            sb = new StringBuilder(tagPrefix);
            sb.append(",");
        } else {
            if (tags == null || tags.length == 0) {
                return "";
            }
            sb = new StringBuilder("|#");
        }

        for (int n = tags.length - 1; n >= 0; n--) {
            sb.append(tags[n]);
            if (n > 0) {
                sb.append(",");
            }
        }
        return sb.toString();
    }

    String tagString(final String[] tags) {
        return tagString(tags, constantTagsRendered);
    }

    @Override
    public void count(String aspect, int delta, String... tags) {
        send(String.format("%s%s:%d|c%s", prefix, aspect, delta, tagString(tags)));
    }

    @Override
    public void incrementCounter(String aspect, String... tags) {
        count(aspect, 1, tags);
    }

    @Override
    public void increment(String aspect, String... tags) {
        incrementCounter(aspect, tags);
    }

    @Override
    public void decrementCounter(String aspect, String... tags) {
        count(aspect, -1, tags);
    }

    @Override
    public void decrement(String aspect, String... tags) {
        decrementCounter(aspect, tags);
    }

    @Override
    public void recordGaugeValue(String aspect, double value, String... tags) {
        /* Intentionally using %s rather than %f here to avoid
         * padding with extra 0s to represent precision */
        send(String.format("%s%s:%s|g%s", prefix, aspect, NUMBER_FORMATTERS.get().format(value), tagString(tags)));
    }

    @Override
    public void gauge(String aspect, double value, String... tags) {
        recordGaugeValue(aspect, value, tags);
    }


    @Override
    public void recordGaugeValue(String aspect, int value, String... tags) {
        send(String.format("%s%s:%d|g%s", prefix, aspect, value, tagString(tags)));
    }

    @Override
    public void gauge(String aspect, int value, String... tags) {
        recordGaugeValue(aspect, value, tags);
    }

    @Override
    public void recordExecutionTime(String aspect, long timeInMs, String... tags) {
        send(String.format("%s%s:%d|ms%s", prefix, aspect, timeInMs, tagString(tags)));
    }

    @Override
    public void time(String aspect, long value, String... tags) {
        recordExecutionTime(aspect, value, tags);
    }

    @Override
    public void recordHistogramValue(String aspect, double value, String... tags) {
        send(String.format("%s%s:%s|h%s", prefix, aspect, NUMBER_FORMATTERS.get().format(value), tagString(tags)));
    }

    @Override
    public void histogram(String aspect, double value, String... tags) {
        recordHistogramValue(aspect, value, tags);
    }

    @Override
    public void recordHistogramValue(String aspect, int value, String... tags) {
        send(String.format("%s%s:%d|h%s", prefix, aspect, value, tagString(tags)));
    }

    @Override
    public void histogram(String aspect, int value, String... tags) {
        recordHistogramValue(aspect, value, tags);
    }

    private void send(String message) {
        if (!disruptor.getRingBuffer().tryPublishEvent(TRANSLATOR, message)) {
            errorHandler.handle(InsufficientCapacityException.INSTANCE);
        }
    }

    static class Event {

        private String value;

        public void setValue(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "Event: " + value;
        }
    }

    private class Handler implements EventHandler<Event> {

        private final ByteBuffer sendBuffer = ByteBuffer.allocate(PACKET_SIZE_BYTES);

        @Override
        public void onEvent(Event event, long sequence, boolean batchEnd) throws Exception {
            String message = event.value;
            byte[] data = message.getBytes();
            if (sendBuffer.remaining() < (data.length + 1)) {
                flush();
            }
            if (sendBuffer.position() > 0) {
                sendBuffer.put((byte) '\n');
            }
            sendBuffer.put(
                    data.length > sendBuffer.remaining() ? Arrays.copyOfRange(data, 0, sendBuffer.remaining()) : data);

            if (batchEnd || 0 == sendBuffer.remaining()) {
                flush();
            }
        }

        private void flush() throws IOException {
            int sizeOfBuffer = sendBuffer.position();
            sendBuffer.flip();
            int sentBytes = clientChannel.send(sendBuffer, address);
            sendBuffer.clear();

            if (sizeOfBuffer != sentBytes) {
                errorHandler.handle(
                        new IOException(
                                String.format(
                                        "Could not send entirely stat %s to host %s:%d. Only sent %d bytes out of %d bytes",
                                        sendBuffer.toString(),
                                        address.getHostName(),
                                        address.getPort(),
                                        sentBytes,
                                        sizeOfBuffer)));
            }
        }
    }

    private static class DisruptorExceptionHandler implements ExceptionHandler {

        private final FatalExceptionHandler throwableHandler = new FatalExceptionHandler();
        private final StatsDClientErrorHandler exceptionHandler;

        public DisruptorExceptionHandler(StatsDClientErrorHandler handler) {
            this.exceptionHandler = handler;
        }

        @Override
        public void handleEventException(Throwable ex, long sequence, Object event) {
            if (ex instanceof Exception) {
                exceptionHandler.handle((Exception) ex);
            } else {
                throwableHandler.handleEventException(ex, sequence, event);
            }
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            if (ex instanceof Exception) {
                exceptionHandler.handle((Exception) ex);
            } else {
                throwableHandler.handleOnStartException(ex);
            }
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            if (ex instanceof Exception) {
                exceptionHandler.handle((Exception) ex);
            } else {
                throwableHandler.handleOnShutdownException(ex);
            }
        }
    }
}
