package io.numaproj.numaflow.function.mapt;

import io.numaproj.numaflow.function.MessageT;
import io.numaproj.numaflow.function.v1.Udfunction;
import java.util.function.BiFunction;

/**
 * Implementation of MapHandler instantiated from a function
 */
public class MapTFunc implements MapTHandler {

    private final BiFunction<String, Udfunction.Datum, MessageT[]> mapTFn;

    public MapTFunc(BiFunction<String, Udfunction.Datum, MessageT[]> mapTFn) {
        this.mapTFn = mapTFn;
    }

    @Override
    public MessageT[] HandleDo(String key, Udfunction.Datum datum) {
        return mapTFn.apply(key, datum);
    }
}
