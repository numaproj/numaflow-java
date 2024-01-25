package io.numaproj.numaflow.sessionreducer;

import com.google.protobuf.Timestamp;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Instant;

/**
 * ActorRequest is used by the supervisor actor to distribute session reduce operations to individual session reducer actors.
 * One actor request is sent to only one session reducer actor.
 */
@Getter
@AllArgsConstructor
class ActorRequest {
    ActorRequestType type;
    Sessionreduce.KeyedWindow keyedWindow;
    Sessionreduce.SessionReduceRequest.Payload payload;

    public String getUniqueIdentifier() {
        long startMillis = convertToEpochMilli(this.keyedWindow.getStart());
        long endMillis = convertToEpochMilli(this.keyedWindow.getEnd());
        return String.format(
                "%d:%d:%s",
                startMillis,
                endMillis,
                String.join(Constants.DELIMITER, this.keyedWindow.getKeysList()));
    }

    private long convertToEpochMilli(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).toEpochMilli();
    }
}
