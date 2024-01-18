package io.numaproj.numaflow.reducestreamer;

public enum ActorResponseType {
    // EOF_RESPONSE indicates that the actor response contains an EOF reduce response without real data.
    EOF_RESPONSE,
    // READY_FOR_CLEAN_UP_SIGNAL indicates that the actor has finished sending responses and now ready to be cleaned up.
    READY_FOR_CLEAN_UP_SIGNAL,
}
