package io.numaproj.numaflow.function.reduce;

import io.numaproj.numaflow.function.HandlerDatum;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.metadata.Metadata;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class GroupBy {
    private String key;
    private Metadata metadata;

    public abstract void readMessage(HandlerDatum handlerDatum);

    public abstract Message[] getResult();
}
