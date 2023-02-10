package io.numaproj.numaflow.function.reduce;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.Message;
import io.numaproj.numaflow.function.metadata.Metadata;
import lombok.AllArgsConstructor;

/**
 * GroupBy exposes methods for performing reduce operation.
 */


@AllArgsConstructor
public abstract class GroupBy {
    public String key;
    public Metadata metadata;

    public abstract void addMessage(Datum datum);

    public abstract Message[] getOutput();
}
