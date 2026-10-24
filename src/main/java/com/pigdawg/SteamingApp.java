package com.pigdawg;

import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.responses.CreateGroupCursorResponse;
import com.oracle.bmc.streaming.responses.CreateStreamPoolResponse;
import com.oracle.bmc.streaming.responses.CreateStreamResponse;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SteamingApp {
    private static final Logger LOG = LoggerFactory.getLogger(SteamingApp.class);

    private static final Properties APP_PROPERTIES = ConfigLoader.loadApplicationProperties(SteamingApp.class);

    private static final String ENDPOINT_OCI_STREAMING = ConfigLoader.getRequiredProperty(APP_PROPERTIES, "oci.streaming.endpoint");
    private static final String ENDPOINT_OCI_STREAMING_ADMIN = ConfigLoader.getAdminEndpoint(APP_PROPERTIES);
    private static final String ENDPOINT_OCI_IDENTITY = ConfigLoader.getRequiredProperty(APP_PROPERTIES, "oci.identity.endpoint");
    private static final String TENANT_STREAMING = ConfigLoader.getRequiredProperty(APP_PROPERTIES, "oci.streaming.tenancy");
    private static final String COMPARTMENT = ConfigLoader.getRequiredProperty(APP_PROPERTIES, "oci.streaming.compartment");
    private static final String AUTH_PROFILE = ConfigLoader.getRequiredProperty(APP_PROPERTIES, "oci.auth.profile");

    private static final long RESOURCE_CREATE_TIMEOUT_MS = Duration.ofMinutes(5).toMillis();
    private static final long RESOURCE_DELETE_TIMEOUT_MS = Duration.ofMinutes(5).toMillis();
    private static final int MESSAGE_COUNT = 10;
    private static final long CONSUMER_TIMEOUT_MS = Duration.ofMinutes(2).toMillis();

    public static void main(String[] args) throws IOException, InterruptedException {
        try {
            LOG.info("SteamingApp starting");
            LocalDateTime now = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
            final String runSuffix = now.format(formatter) + "-" + UUID.randomUUID().toString().substring(0, 8);

            final String poolName = String.format("rest-pool-%s", runSuffix);
            final String streamName = String.format("rest-stream-%s", runSuffix);
            final String consumerGroupName = String.format("rest-consumergroup-%s", runSuffix);
            final String cursorName = String.format("rest-cursor-%s", runSuffix);
            LOG.info("Generated names pool={} stream={} consumerGroup={} cursor={}", poolName, streamName, consumerGroupName, cursorName);

            OciStreamingHelper.validateAuthenticationToken(ENDPOINT_OCI_IDENTITY, AUTH_PROFILE, TENANT_STREAMING);

            StreamClient streamClient = OciStreamingHelper.createStreamClient(ENDPOINT_OCI_STREAMING, AUTH_PROFILE);
            StreamAdminClient adminClient = OciStreamingHelper.createStreamAdminClient(ENDPOINT_OCI_STREAMING_ADMIN, AUTH_PROFILE);
            LOG.info("Initialized StreamClient for endpoint={}", ENDPOINT_OCI_STREAMING);
            LOG.info("Initialized StreamAdminClient for endpoint={}", ENDPOINT_OCI_STREAMING_ADMIN);

            String streamPoolId = null;
            String streamId = null;
            Exception primaryFailure = null;
            try {
                LOG.info("Creating stream pool {} in compartment {}", poolName, COMPARTMENT);
                CreateStreamPoolResponse createStreamPoolResponse =
                        OciStreamingHelper.createStreamPool(adminClient, COMPARTMENT, poolName);
                streamPoolId = createStreamPoolResponse.getStreamPool().getId();
                LOG.info("Created stream pool id={}", streamPoolId);

                OciStreamingHelper.waitForStreamPoolToBecomeActive(adminClient, streamPoolId, RESOURCE_CREATE_TIMEOUT_MS);

                CreateStreamResponse createStreamResponse =
                        OciStreamingHelper.createStream(adminClient, streamPoolId, streamName);
                streamId = createStreamResponse.getStream().getId();
                LOG.info("Created stream id={} in pool={}", streamId, streamPoolId);

                OciStreamingHelper.waitForStreamToBecomeActive(adminClient, streamId, RESOURCE_CREATE_TIMEOUT_MS);

                final long startMs = System.currentTimeMillis();
                final long consumerDeadlineMs = startMs + CONSUMER_TIMEOUT_MS;

                // A cursor can be created as part of a consumer group.
                // Committed offsets are managed for the group, and partitions
                // are dynamically balanced amongst consumers in the group.
                CreateGroupCursorResponse groupCursorResponse = OciStreamingHelper.createGroupCursor(streamClient, streamId, consumerGroupName, cursorName);
                String cursor = groupCursorResponse.getCursor().getValue();
                LOG.debug("Consumer cursor created for stream={}", streamId);

                ProducerConsumerThreads producerConsumerThreads =
                        new ProducerConsumerThreads(
                                streamClient,
                                streamId,
                                cursor,
                                MESSAGE_COUNT,
                                consumerDeadlineMs);
                Thread producerThread = producerConsumerThreads.createProducerThread();
                Thread consumerThread = producerConsumerThreads.createConsumerThread();

                LOG.info("Starting REST producer and consumer threads for stream={}", streamId);
                consumerThread.start();
                producerThread.start();
                producerThread.join();
                consumerThread.join();
                producerConsumerThreads.throwIfFailed();
                LOG.info(
                        "Threads completed. produced={} consumed={}",
                        producerConsumerThreads.getProducedCount(),
                        producerConsumerThreads.getConsumedCount());
            } catch (RuntimeException | InterruptedException ex) {
                primaryFailure = ex;
                throw ex;
            } finally {
                cleanupResources(adminClient, streamPoolId, streamId, primaryFailure);
            }
        } finally {
            LOG.info("SteamingApp done");
        }
    }

    private static void cleanupResources(
            StreamAdminClient adminClient,
            String streamPoolId,
            String streamId,
            Exception primaryFailure)
            throws InterruptedException {
        Exception cleanupFailure = null;
        boolean streamDeleted = streamId == null;

        if (streamId != null) {
            try {
                LOG.info("Deleting stream id={}", streamId);
                OciStreamingHelper.deleteStream(adminClient, streamId);
                OciStreamingHelper.waitForStreamToBecomeDeleted(adminClient, streamId, RESOURCE_DELETE_TIMEOUT_MS);
                streamDeleted = true;
            } catch (RuntimeException | InterruptedException ex) {
                cleanupFailure = rememberCleanupFailure(primaryFailure, cleanupFailure, ex);
                LOG.warn("Stream deletion was not confirmed. streamId={}", streamId, ex);
            }
        }

        if (streamPoolId != null) {
            if (streamDeleted) {
                try {
                    LOG.info("Deleting stream pool id={}", streamPoolId);
                    OciStreamingHelper.deleteStreamPoolWhenEmpty(adminClient, streamPoolId, RESOURCE_DELETE_TIMEOUT_MS);
                    OciStreamingHelper.waitForStreamPoolToBecomeDeleted(adminClient, streamPoolId, RESOURCE_DELETE_TIMEOUT_MS);
                } catch (RuntimeException | InterruptedException ex) {
                    cleanupFailure = rememberCleanupFailure(primaryFailure, cleanupFailure, ex);
                    LOG.warn("Stream pool deletion failed. streamPoolId={}", streamPoolId, ex);
                }
            } else {
                LOG.warn("Skipping stream pool deletion. streamPoolId={} reason=stream deletion was not confirmed", streamPoolId);
            }
        }

        throwCleanupFailureIfNoPrimaryFailure(primaryFailure, cleanupFailure);
    }

    private static Exception rememberCleanupFailure(
            Exception primaryFailure,
            Exception existingCleanupFailure,
            Exception cleanupFailure) {
        if (cleanupFailure instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }

        if (primaryFailure != null) {
            primaryFailure.addSuppressed(cleanupFailure);
            return existingCleanupFailure;
        }

        if (existingCleanupFailure != null) {
            existingCleanupFailure.addSuppressed(cleanupFailure);
            return existingCleanupFailure;
        }

        return cleanupFailure;
    }

    private static void throwCleanupFailureIfNoPrimaryFailure(
            Exception primaryFailure,
            Exception cleanupFailure)
            throws InterruptedException {
        if (primaryFailure != null || cleanupFailure == null) {
            return;
        }

        if (cleanupFailure instanceof InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw interruptedException;
        }

        if (cleanupFailure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }

        throw new IllegalStateException("Cleanup failed", cleanupFailure);
    }
}
