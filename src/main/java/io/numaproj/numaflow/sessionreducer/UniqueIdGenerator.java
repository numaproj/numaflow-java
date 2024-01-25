package io.numaproj.numaflow.sessionreducer;

import com.google.protobuf.Timestamp;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;

import java.time.Instant;

public class UniqueIdGenerator {
    private UniqueIdGenerator() {
        throw new AssertionError("Utility class cannot be instantiated");
    }

    public static String getUniqueIdentifier(Sessionreduce.KeyedWindow keyedWindow) {
        long startMillis = convertToEpochMilli(keyedWindow.getStart());
        long endMillis = convertToEpochMilli(keyedWindow.getEnd());
        return String.format(
                "%d:%d:%s",
                startMillis,
                endMillis,
                String.join(Constants.DELIMITER, keyedWindow.getKeysList()));
    }

    private static long convertToEpochMilli(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).toEpochMilli();
    }
}
