package io.numaproj.numaflow.shared;

import lombok.Builder;
import lombok.Getter;

import java.util.Map;

/**
 * NackOptions carries per-message redelivery options for a negative acknowledgement (nack).
 * All fields are optional; a null value means unset.
 */
@Getter
@Builder(builderMethodName = "newBuilder")
public class NackOptions {
    /** redelivery delay in milliseconds. */
    private final Long delay;
    /** maximum number of redelivery attempts. */
    private final Integer maxDeliveries;
    /** human-readable reason for the nack. */
    private final String reason;
    /** generic values passed as nack options */
    private final Map<String, String> nackMap;

    /** Converts to the outgoing proto type, setting only the fields that are present. */
    public common.NackOptionsOuterClass.NackOptions toProto() {
        common.NackOptionsOuterClass.NackOptions.Builder b =
                common.NackOptionsOuterClass.NackOptions.newBuilder();
        if (delay != null) {
            b.setDelay(delay);
        }
        if (maxDeliveries != null) {
            b.setMaxDeliveries(maxDeliveries);
        }
        if (reason != null) {
            b.setReason(reason);
        }
        if (nackMap != null) {
            b.putAllNackMap(nackMap);
        }
        return b.build();
    }

    /** Converts from the incoming proto type. Returns null for null input. */
    public static NackOptions fromProto(common.NackOptionsOuterClass.NackOptions p) {
        if (p == null) {
            return null;
        }
        NackOptionsBuilder b = NackOptions.newBuilder();
        if (p.hasDelay()) {
            b.delay(p.getDelay());
        }
        if (p.hasMaxDeliveries()) {
            b.maxDeliveries(p.getMaxDeliveries());
        }
        if (p.hasReason()) {
            b.reason(p.getReason());
        }
        if (!p.getNackMapMap().isEmpty()) {
            b.nackMap(p.getNackMapMap());
        }
        return b.build();
    }
}
