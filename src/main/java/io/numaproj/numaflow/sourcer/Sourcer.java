package io.numaproj.numaflow.sourcer;

import java.util.Collections;
import java.util.List;

/**
 * Sourcer exposes method for reading messages from source.
 * Implementations should override the read method which will be used
 * for reading messages from source and ack method which will be used
 * for acknowledging the messages read from source and pending method
 * which will be used for getting the number of pending messages in the
 */
public abstract class Sourcer {
    /**
     * method will be used for reading messages from source.
     *
     * @param request the request
     * @param observer the observer for the output
     */
    public abstract void read(ReadRequest request, OutputObserver observer);

    /**
     * method will be used for acknowledging messages from source.
     *
     * @param request the request containing the offsets to be acknowledged
     */
    public abstract void ack(AckRequest request);

    /**
     * method will be used for getting the number of pending messages from source.
     * when the return value is negative, it indicates the pending information is not available.
     *
     * @return number of pending messages
     */
    public abstract long getPending();

    /**
     * method returns the partitions associated with the source, will be used by the platform to determine
     * the partitions to which the watermark should be published. If the source doesn't have partitions,
     * `defaultPartitions()` can be used to return the default partitions.
     * In most cases, the defaultPartitions() should be enough; the cases where we need to implement custom getPartitions()
     * is in a case like Kafka, where a reader can read from multiple Kafka partitions.
     *
     * @return list of partitions
     */
    public abstract List<Integer> getPartitions();

    /**
     * method returns default partitions for the source.
     * It can be used in the getPartitions() function of the Sourcer interface only
     * if the source doesn't have partitions. DefaultPartition will be the pod replica
     * index of the source.
     *
     * @return list of partitions
     */
    public static List<Integer> defaultPartitions() {
        String partition = System.getenv().getOrDefault("NUMAFLOW_REPLICA", "0");
        return Collections.singletonList(Integer.parseInt(partition));
    }
}
