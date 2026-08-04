package io.numaproj.numaflow.shared;

/**
 * Message sent by gRPC services to supervisor actors when the inbound request stream fails.
 */
public class InputStreamError {
    private final Throwable cause;

    public InputStreamError(Throwable cause) {
        this.cause = cause;
    }

    public Throwable getCause() {
        return cause;
    }
}
