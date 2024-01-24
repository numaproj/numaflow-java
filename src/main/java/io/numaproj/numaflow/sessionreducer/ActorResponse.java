package io.numaproj.numaflow.sessionreducer;

import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

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
    ReduceOuterClass.ReduceResponse response;
    boolean isLast;

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
