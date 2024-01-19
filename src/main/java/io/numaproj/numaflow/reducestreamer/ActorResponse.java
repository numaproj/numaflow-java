package io.numaproj.numaflow.reducestreamer;

import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * ActorResponse is for child reduce streamer actors to report back to the supervisor actor about the status of the data processing.
 * It serves two purposes:
 * 1. Send to the supervisor an EOF response, which is to be sent to the gRPC output stream.
 * 2. Send to the supervisor a signal, indicating that the actor has finished all its processing work,
 * and it's ready to be cleaned up by the supervisor actor.
 */
@Getter
@AllArgsConstructor
class ActorResponse {
    ReduceOuterClass.ReduceResponse response;
    ActorResponseType type;

    // TODO - do we need to include window information in the id?
    // for aligned reducer, there is always single window.
    // but at the same time, would like to be consistent with GO SDK implementation.
    // we will revisit this one later.
    public String getActorUniqueIdentifier() {
        return String.join(
                Constants.DELIMITER,
                this.getResponse().getResult().getKeysList().toArray(new String[0]));
    }
}
