package io.numaproj.numaflow.function.reduce;

import io.numaproj.numaflow.function.v1.Udfunction;

/**
 * Interface for reduce datum stream which is passed
 * to reduce handler, udf owners can read the message
 * using ReadMessage function, and it returns null if
 * there are no more messages to consume.
 */
public interface ReduceDatumStream {
    public Udfunction.Datum ReadMessage();
}
