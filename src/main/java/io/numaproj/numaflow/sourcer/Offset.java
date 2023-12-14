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
    private final Integer partitionId;

    /**
     * used to create Offset with value and partitionId.
     *
     * @param value offset value
     * @param partitionId offset partitionId
     */
    public Offset(byte[] value, Integer partitionId) {
        this.value = value;
        this.partitionId = partitionId;
    }

    /**
     * used to create Offset with value and default partitionId.
     *
     * @param value offset value
     */
    public Offset(byte[] value) {
        this.value = value;
        this.partitionId = Sourcer.defaultPartitions().get(0);
    }
}
