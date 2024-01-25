package io.numaproj.numaflow.sessionreducer;

import com.google.protobuf.Timestamp;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.time.Instant;

/**
 * The actor response holds the final EOF response for a particular key set.
 * <p>
 * The isLast attribute indicates whether the response is globally the last one to be sent to
 * the output gRPC stream, if set to true, it means the response is the very last response among
 * all key sets. When output stream actor receives an isLast response, it sends the response and immediately
 * closes the output stream.
 */
@Getter
@Setter
@AllArgsConstructor
class ActorResponse {
    Sessionreduce.SessionReduceResponse response;
    boolean isLast;

    public String getActorUniqueIdentifier() {
        long startMillis = convertToEpochMilli(this.response.getKeyedWindow().getStart());
        long endMillis = convertToEpochMilli(this.response.getKeyedWindow().getEnd());
        return String.format(
                "%d:%d:%s",
                startMillis,
                endMillis,
                String.join(Constants.DELIMITER, this.response.getKeyedWindow().getKeysList()));
    }

    private long convertToEpochMilli(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).toEpochMilli();
    }
}
