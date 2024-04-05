package io.numaproj.numaflow.sinker;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.util.ArrayList;
import java.util.List;

/**
 * ResponseList is used to return the list of responses from user defined sinker.
 */

@Getter
@Builder(builderMethodName = "newBuilder")
public class ResponseList {

    @Singular("addResponse")
    private List<Response> responses;

    /**
     * Builder to build ResponseList
     */
    public static class ResponseListBuilder {
        /**
         * @param responses to append all the responses to ResponseList
         *
         * @return returns the builder
         */
        public ResponseListBuilder addResponses(Iterable<Response> responses) {
            if (this.responses == null) {
                this.responses = new ArrayList<>();
            }

            for (Response response: responses) {
                this.responses.add(response);
            }
            return this;
        }
    }
}
