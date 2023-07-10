package io.numaproj.numaflow.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ThreadUtils is a utility class for thread related operations.
 */
@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class ThreadUtils {
    public static final ThreadUtils INSTANCE = new ThreadUtils();


    /**
     * availableProcessors returns the number available processors.
     * @return the number of available processors
     */
    public int availableProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }

    /**
     * Creates a new thread factory with the given name.
     *
     * @param name the name of the thread
     * @return a new thread factory
     */
    public ThreadFactory newThreadFactory(String name) {
        return new ThreadFactory() {
            private final AtomicInteger num = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                int s = num.getAndIncrement();
                t.setName(s > 0 ? name + "-" + s : name);
                return t;
            }
        };
    }
}
