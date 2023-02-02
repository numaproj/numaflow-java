package io.numaproj.numaflow.function.mapt;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.MessageT;

import java.util.function.BiFunction;

/**
 * Implementation of MapTHandler instantiated from a function
 */
public class MapTFunc implements MapTHandler {

    private final BiFunction<String, Datum, MessageT[]> mapTFn;

    public MapTFunc(BiFunction<String, Datum, MessageT[]> mapTFn) {
        this.mapTFn = mapTFn;
    }

    @Override
    public MessageT[] HandleDo(String key, Datum datum) {
        return mapTFn.apply(key, datum);
    }
}
