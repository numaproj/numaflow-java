package io.numaproj.numaflow.sessionreducer.model;

/**
 * ReduceStreamerFactory is the factory for SessionReducer.
 */
public abstract class SessionReducerFactory<SessionReducerT extends SessionReducer> {
    /**
     * Helper function to create a session reducer.
     *
     * @return a concrete session reducer instance
     */
    public abstract SessionReducerT createSessionReducer();
}
