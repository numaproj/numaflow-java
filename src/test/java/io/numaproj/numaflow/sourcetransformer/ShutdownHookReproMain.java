package io.numaproj.numaflow.sourcetransformer;

import io.grpc.inprocess.InProcessServerBuilder;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Helper entry point launched in a *forked* JVM by {@link ServerShutdownHookTest}.
 * <p>
 * It starts a real {@link Server} with {@code isLocal=false} so that
 * {@link Server#start()} registers the JVM shutdown hook, then triggers a normal
 * JVM shutdown from the main thread (exactly like the surefire booter does at the
 * end of a test run).
 * <p>
 * With the buggy hook - which calls {@code System.exit(0)} from *inside* the hook -
 * the JVM self-deadlocks: the thread running the hooks holds the {@code Shutdown}
 * lock and waits for the hook to finish, while the hook blocks in {@code Shutdown.exit}
 * waiting for that same lock. The process then hangs forever. With the fix (hook only
 * calls {@code stop()}) the JVM exits cleanly with code 0.
 */
public class ShutdownHookReproMain {

    public static void main(String[] args) throws Exception {
        Path tmpDir = Files.createTempDirectory("st-shutdown-repro");
        String socketPath = tmpDir.resolve("sourcetransform.sock").toString();
        String infoFilePath = tmpDir.resolve("server-info").toString();

        GRPCConfig config = GRPCConfig.newBuilder()
                .maxMessageSize(Constants.DEFAULT_MESSAGE_SIZE)
                .socketPath(socketPath)
                .infoFilePath(infoFilePath)
                // isLocal=false is what makes Server.start() register the shutdown hook.
                .isLocal(false)
                .build();

        // Use the in-process (VisibleForTesting) server so we don't need to bind a
        // real unix-domain socket; the shutdown hook is registered regardless.
        Server server = new Server(
                config,
                new NoopSourceTransformer(),
                null,
                InProcessServerBuilder.generateName());

        server.start();

        // Signal to the parent that startup succeeded, so a failure to exit can be
        // attributed to the shutdown hook rather than a startup problem.
        System.out.println("SERVER_STARTED");
        System.out.flush();

        // Initiate a normal JVM shutdown from the main thread. This fires the hook
        // registered by Server.start(). If the hook calls System.exit(), we deadlock.
        System.exit(0);
    }

    private static final class NoopSourceTransformer extends SourceTransformer {
        @Override
        public MessageList processMessage(String[] keys, Datum datum) {
            return MessageList.newBuilder().build();
        }
    }
}
