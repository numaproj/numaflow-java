package io.numaproj.numaflow.reducestreamer;

import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * ActorEOFResponse is to store the EOF signal from a ReduceStreamerActor.
 * ReduceStreamerActor sends it back to the supervisor actor to indicate that
 * the streamer actor itself has finished processing the data and is ready to be
 * released.
 */
@Getter
@AllArgsConstructor
class ActorEOFResponse {
    ReduceOuterClass.ReduceResponse response;

    // TODO - do we need to include window information in the id?
    // for aligned reducer, there is always single window.
    // but at the same time, would like to be consistent with GO SDK implementation.
    // we will revisit this one later.
    public String getUniqueIdentifier() {
        return String.join(
                Constants.DELIMITER,
                this.getResponse().getResult().getKeysList().toArray(new String[0]));
    }
}
