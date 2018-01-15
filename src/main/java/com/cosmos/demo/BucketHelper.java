package com.cosmos.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

/**
 * Helper class to manage a pool of connections to Couchbase.
 */
public class BucketHelper<T> {

    private static final Logger logger = LoggerFactory.getLogger(BucketHelper.class);

    public static final String STATUS_OK = "ok";
    public static final String STATUS_FAILED = "failed";

    private final T[] buckets;

    private final Optional<String> state;
    private Throwable failure;

    /**
     * Convenient constructor.
     *
     * @param size     size of the connection pool (minimum 1)
     * @param state    is state required
     * @param supplier T constructor
     */
    public BucketHelper(final int size, final String state, final Supplier<T> supplier) {
        final int capacity = Math.max(1, size);
        buckets = (T[]) new Object[capacity];
        for (int i = 0; i < capacity; i++) {
            try {
                buckets[i] = supplier.get();
            } catch (Throwable e) {
                logger.error(String.format("Failed to initialize to bucket [idx=%d]", i), e);
                this.state = of(STATUS_FAILED);
                failure = e;
                return;
            }
        }
        this.state = of(state);
    }

    /**
     * Gets a bucket.
     *
     * @return A bucket.
     */
    public T bucket() {
        final int idx = ThreadLocalRandom.current().nextInt(buckets.length);
        final T bucket = buckets[idx];
        return ofNullable(bucket).orElseThrow(() -> new IllegalStateException(String.format("No bucket available [idx=%d]", idx)));
    }

    /**
     * Closes this bucket helper with a custom timeout.
     */
    public void close(Consumer<T> t) {
        logger.info("Closing buckets. Current size: " + (buckets == null ? 0 : buckets.length));
        for (T values : buckets) {
            try {
                t.accept(values);
            } catch (Exception ignore) {
            }
        }
    }

    // TODO use enums and review usage of state by other processes
    public Optional<String> getState() {
        return state;
    }

    public Throwable getFailure() {
        return failure;
    }
}