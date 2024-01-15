package io.numaproj.numaflow.reducestreamer;

import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * ActorResponse is to store the response from ReduceActors.
 */
@Getter
@AllArgsConstructor
class ActorResponse {
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
