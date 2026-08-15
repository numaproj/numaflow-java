package io.numaproj.numaflow.mapper;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Reproduces the shutdown-hook self-deadlock fixed by removing {@code System.exit(0)}
 * from the JVM shutdown hook in {@link Server}.
 * <p>
 * The deadlock only manifests at JVM exit, so it cannot be observed from a normal
 * in-process test method. Instead we fork a child JVM ({@link ShutdownHookReproMain})
 * that starts a real {@link Server} (which registers the hook) and then initiates
 * shutdown, and we assert the process actually terminates within a timeout.
 * <p>
 * Expected results:
 * <ul>
 *   <li>buggy code (hook calls {@code System.exit(0)}): child JVM hangs -> this test FAILS.</li>
 *   <li>fixed code (hook only calls {@code stop()}): child JVM exits 0 -> this test PASSES.</li>
 * </ul>
 */
public class ServerShutdownHookTest {

    private static final long EXIT_TIMEOUT_SECONDS = 20;

    @Test
    public void shutdownHookMustNotDeadlockJvmExit() throws Exception {
        String javaBin = System.getProperty("java.home")
                + File.separator + "bin" + File.separator + "java";
        String classpath = System.getProperty("java.class.path");

        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "-cp", classpath,
                ShutdownHookReproMain.class.getName());
        pb.redirectErrorStream(true);

        Process process = pb.start();

        // Drain the child's output on a background thread so its pipe buffer can never
        // fill up and block the child (which would be a false-positive "hang").
        StringBuilder output = new StringBuilder();
        Thread drainer = new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    synchronized (output) {
                        output.append(line).append('\n');
                    }
                }
            } catch (Exception ignored) {
                // stream closed on process exit
            }
        });
        drainer.setDaemon(true);
        drainer.start();

        boolean exited = process.waitFor(EXIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        if (!exited) {
            process.destroyForcibly();
            process.waitFor(10, TimeUnit.SECONDS);
            drainer.join(TimeUnit.SECONDS.toMillis(2));
            synchronized (output) {
                fail("Forked JVM failed to exit within " + EXIT_TIMEOUT_SECONDS
                        + "s: the shutdown hook self-deadlocked (System.exit() called "
                        + "from within the hook). Child output:\n" + output);
            }
        }

        drainer.join(TimeUnit.SECONDS.toMillis(2));
        String childOutput;
        synchronized (output) {
            childOutput = output.toString();
        }

        // Make sure the child actually got far enough to register the hook; otherwise a
        // clean exit would be meaningless (e.g. an unrelated startup failure).
        assertTrue(
                "Child JVM did not report a successful server start; cannot trust the exit "
                        + "result. Child output:\n" + childOutput,
                childOutput.contains("SERVER_STARTED"));

        assertEquals(
                "Forked JVM should exit cleanly (exit code 0). Child output:\n" + childOutput,
                0,
                process.exitValue());
    }
}
