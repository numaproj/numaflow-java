package io.numaproj.numaflow.sink;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Response {
    private final String id;
    private final Boolean success;
    private final String err;
}
