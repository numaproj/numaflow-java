package io.numaproj.numaflow.function;

import io.numaproj.numaflow.function.v1.Udfunction;

import java.util.function.BiFunction;

/**
 * Implementation of MapHandler instantiated from a function
 */
public class MapFunc implements MapHandler {
  private final BiFunction<String, Udfunction.Datum, Message[]> mapFn;

  public MapFunc(BiFunction<String, Udfunction.Datum, Message[]> mapFn) {
    this.mapFn = mapFn;
  }

  @Override
  public Message[] HandleDo(String key, Udfunction.Datum datum) {
    return mapFn.apply(key, datum);
  }
}
