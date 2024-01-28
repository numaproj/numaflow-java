package io.numaproj.numaflow.sessionreducer;

import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * The actor response holds the session reduce response from a particular session window.
 */
@Getter
@Setter
class ActorResponse {
    Sessionreduce.SessionReduceResponse response;
    // The isLast attribute indicates whether the response is globally the last one to be sent to
    // the output gRPC stream, if set to true, it means the response is the very last response among
    // all windows. When output actor receives an isLast response, it sends the response to and immediately
    // closes the output stream.
    boolean isLast;
    // The accumulator attribute holds the accumulator of the session.
    byte[] accumulator;
    // The mergeTaskId attribute holds the merge task id that this session is to be merged into.
    String mergeTaskId;

    @Builder
    private ActorResponse(
            Sessionreduce.SessionReduceResponse response,
            boolean isLast,
            byte[] accumulator,
            String mergeTaskId
    ) {
        this.response = response;
        this.isLast = isLast;
        this.accumulator = accumulator;
        this.mergeTaskId = mergeTaskId;
    }

    static class ActorResponseBuilder {
        ActorResponse build() {
            if ((accumulator != null && mergeTaskId == null) || (accumulator == null
                    && mergeTaskId != null)) {
                throw new IllegalStateException(
                        "attributes accumulator and mergeTaskId should be either both null or both non-null.");
            }
            return new ActorResponse(response, isLast, accumulator, mergeTaskId);
        }
    }

    boolean isEOFResponse() {
        return this.accumulator == null && this.mergeTaskId == null;
    }
}
