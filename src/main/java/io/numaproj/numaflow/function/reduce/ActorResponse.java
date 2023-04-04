package io.numaproj.numaflow.function.reduce;

import io.numaproj.numaflow.function.v1.Udfunction;
import lombok.AllArgsConstructor;
import lombok.Getter;
/*
    used to store the reduced result from the handler
*/
@Getter
@AllArgsConstructor
public class ActorResponse {
    String[] keys;
    Udfunction.DatumList datumList;
}
