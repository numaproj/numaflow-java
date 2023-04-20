package io.numaproj.numaflow.sink;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.util.Collection;

/**
 * ResponseList is used to return the list of responses from udsink
 */

@Getter
@Builder(builderMethodName = "newBuilder")
public class ResponseList {

    @Singular("addResponse")
    private Iterable<Response> responses;

    public static class ResponseListBuilder {
        public ResponseListBuilder addResponses(Iterable<Response> responses) {
            this.responses.addAll((Collection<? extends Response>) responses);
            return this;
        }
    }
}
