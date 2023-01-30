package io.numaproj.numaflow.function.map;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.Message;

import java.util.function.BiFunction;

/**
 * Implementation of MapHandler instantiated from a function
 */
public class MapFunc implements MapHandler {
    private final BiFunction<String, Datum, Message[]> mapFn;

    public MapFunc(BiFunction<String, Datum, Message[]> mapFn) {
        this.mapFn = mapFn;
    }

    @Override
    public Message[] HandleDo(String key, Datum datum) {
        return mapFn.apply(key, datum);
    }
}
