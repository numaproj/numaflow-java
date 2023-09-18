package io.numaproj.numaflow.sourcer;

import lombok.Getter;
import lombok.Setter;

/**
 * Offset is the message offset.
 */
@Getter
@Setter
public class Offset {
    private final byte[] value;
    private final String partitionId;

    /**
     * used to create Offset with value and partitionId.
     *
     * @param value offset value
     * @param partitionId offset partitionId
     */
    public Offset(byte[] value, String partitionId) {
        this.value = value;
        this.partitionId = partitionId;
    }
}
