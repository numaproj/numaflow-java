package io.numaproj.numaflow.sourcer;

import io.numaproj.numaflow.shared.NackOptions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * NackOffset contains the message offset and the
 * corresponding nack option for the said offset.
 */
@Getter
@Setter
@AllArgsConstructor
public class NackOffset {
    private final Offset offset;
    private final NackOptions nackOptions;

    /**
     * used to create NackOffset with offset and null nackOptions.
     *
     * @param offset offset value
     */
    public NackOffset(Offset offset) {
        this.offset = offset;
        this.nackOptions = null;
    }
}
