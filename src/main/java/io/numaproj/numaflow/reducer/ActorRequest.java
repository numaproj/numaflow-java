package io.numaproj.numaflow.reducer;

import io.numaproj.numaflow.reduce.v1.ReduceOuterClass;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * ActorRequest is to store the request sent to ReduceActors.
 */
@Getter
@AllArgsConstructor
class ActorRequest {
    ReduceOuterClass.ReduceRequest request;

    // TODO - do we need to include window information in the id?
    // for aligned reducer, there is always single window.
    // but at the same time, would like to be consistent with GO SDK implementation.
    // we will revisit this one later.
    public String getUniqueIdentifier() {
        return String.join(
                Constants.DELIMITER,
                this.getRequest().getPayload().getKeysList().toArray(new String[0]));
    }
    
    public String[] getKeySet() {
        return this.getRequest().getPayload().getKeysList().toArray(new String[0]);
    }
}
