package io.numaproj.numaflow.sink;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Response {
    private final String id;
    private final Boolean success;
    private final String err;

    public static Response responseOK(String id) {
        return new Response(id, true, null);
    }

    public static Response responseFailure(String id, String errMsg) {
        return new Response(id, false, errMsg);
    }
}
