package com.pigdawg;

import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.Message;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ProducerConsumerThreads {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerConsumerThreads.class);

    private static final Duration CONSUMER_POLL_PAUSE = Duration.ofMillis(200);
    private static final long PRODUCER_SEND_PAUSE_MS = 200;

    private final StreamClient streamClient;
    private final String streamId;
    private final int messageCount;
    private final long consumerDeadlineMs;
    private final AtomicInteger producedCount = new AtomicInteger(0);
    private final AtomicInteger consumedCount = new AtomicInteger(0);
    private final AtomicBoolean producerDone = new AtomicBoolean(false);
    private final AtomicReference<Exception> threadFailure = new AtomicReference<>();
    private final Set<String> consumedMessages = ConcurrentHashMap.newKeySet();

    private String cursor;

    ProducerConsumerThreads(
            StreamClient streamClient,
            String streamId,
            String cursor,
            int messageCount,
            long consumerDeadlineMs) {
        this.streamClient = streamClient;
        this.streamId = streamId;
        this.cursor = cursor;
        this.messageCount = messageCount;
        this.consumerDeadlineMs = consumerDeadlineMs;
    }

    int getProducedCount() {
        return producedCount.get();
    }

    int getConsumedCount() {
        return consumedCount.get();
    }

    void throwIfFailed() {
        Exception failure = threadFailure.get();
        if (failure == null) {
            throwIfMessagesWereNotDrained();
            return;
        }

        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }

        throw new IllegalStateException("REST producer/consumer thread failed", failure);
    }

    Thread createProducerThread() {
        return new Thread(() -> {
            try {
                LOG.info("REST producer thread started for stream={}", streamId);
                for (int sequence = 1; sequence <= messageCount; sequence++) {
                    String payload = "message-" + sequence;
                    OciStreamingHelper.publishMessage(streamClient, streamId, payload);
                    producedCount.incrementAndGet();
                    LOG.info("Produced: {}", payload);
                    Thread.sleep(PRODUCER_SEND_PAUSE_MS);
                }
            } catch (Exception ex) {
                rememberThreadFailure(ex);
                LOG.error("REST producer thread error for stream={}", streamId, ex);
            } finally {
                producerDone.set(true);
                LOG.info("REST producer thread exiting. totalProduced={}", producedCount.get());
            }
        }, "rest-producer");
    }

    Thread createConsumerThread() {
        return new Thread(() -> {
            try {
                LOG.info("REST consumer thread started for stream={}", streamId);
                while (System.currentTimeMillis() < consumerDeadlineMs) {
                    GetMessagesResponse response = OciStreamingHelper.getMessages(streamClient, streamId, cursor, 10);

                    List<Message> messages = response.getItems();
                    if (messages != null && !messages.isEmpty()) {
                        LOG.debug("REST consumer received {} messages in batch", messages.size());
                        for (Message message : messages) {
                            String value = new String(message.getValue(), StandardCharsets.UTF_8);
                            consumedMessages.add(value);
                            int totalConsumed = consumedCount.incrementAndGet();
                            LOG.info("Consumed ({}) : {}", totalConsumed, value);
                        }
                    } else {
                        LOG.debug("REST consumer poll returned no messages");
                        Thread.sleep(CONSUMER_POLL_PAUSE.toMillis());
                    }

                    if (response.getOpcNextCursor() != null && !response.getOpcNextCursor().isEmpty()) {
                        cursor = response.getOpcNextCursor();
                    }

                    if (producerDone.get() && consumedCount.get() >= producedCount.get()) {
                        break;
                    }
                }

                if (producerDone.get() && consumedCount.get() < producedCount.get()) {
                    LOG.warn(
                            "REST consumer reached timeout before full drain. produced={} consumed={}",
                            producedCount.get(),
                            consumedCount.get());
                }
                LOG.info("REST consumer thread exiting. produced={} consumed={}", producedCount.get(), consumedCount.get());
            } catch (Exception ex) {
                rememberThreadFailure(ex);
                LOG.error("REST consumer thread error for stream={}", streamId, ex);
            }
        }, "rest-consumer");
    }

    private void rememberThreadFailure(Exception failure) {
        Exception existingFailure = threadFailure.get();
        if (existingFailure != null) {
            existingFailure.addSuppressed(failure);
            return;
        }

        if (!threadFailure.compareAndSet(null, failure)) {
            threadFailure.get().addSuppressed(failure);
        }
    }

    private void throwIfMessagesWereNotDrained() {
        int produced = producedCount.get();
        int consumed = consumedCount.get();
        Set<String> expectedMessages = expectedMessages();
        if (consumed < produced || !consumedMessages.equals(expectedMessages)) {
            throw new IllegalStateException(
                    "REST consumer did not drain produced messages. produced=" + produced
                            + " consumed=" + consumed
                            + " expectedMessages=" + expectedMessages
                            + " consumedMessages=" + consumedMessages);
        }
    }

    private Set<String> expectedMessages() {
        return IntStream.rangeClosed(1, messageCount)
                .mapToObj(sequence -> "message-" + sequence)
                .collect(Collectors.toUnmodifiableSet());
    }
}
