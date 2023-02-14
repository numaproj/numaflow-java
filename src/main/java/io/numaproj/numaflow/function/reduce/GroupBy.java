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

    /*
        addMessage will be invoked for each input message.
        It can be used to read the input data from datum and
        update the result for a given key.
     */
    public abstract void addMessage(Datum datum);

    /*
        getOutput will be invoked at the end of input.
        It can is used to return the aggregated result.
     */
    public abstract Message[] getOutput();
}
