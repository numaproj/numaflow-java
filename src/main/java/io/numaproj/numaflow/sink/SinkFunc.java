package io.numaproj.numaflow.sink;

import io.numaproj.numaflow.sink.v1.Udsink;

import java.util.function.Function;

/**
 * Implementation of SinkHandler instantiated from a function
 */
public class SinkFunc implements SinkHandler {
  private final Function<Udsink.Datum[], Response[]> sinkFn;

  public SinkFunc(Function<Udsink.Datum[], Response[]> sinkFn) {
    this.sinkFn = sinkFn;
  }

  @Override
  public Response[] HandleDo(Udsink.Datum[] datumList) {
    return sinkFn.apply(datumList);
  }
}
