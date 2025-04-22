package io.numaproj.numaflow.sessionreducer;

import com.google.protobuf.Timestamp;
import io.numaproj.numaflow.sessionreduce.v1.Sessionreduce;

import java.time.Instant;

/** UniqueIdGenerator is a utility class to generate a unique id for a keyed session window. */
public class UniqueIdGenerator {
  // private constructor to prevent instantiation
  private UniqueIdGenerator() {
    throw new AssertionError("utility class cannot be instantiated");
  }

  public static String getUniqueIdentifier(Sessionreduce.KeyedWindow keyedWindow) {
    long startMillis = convertToEpochMilli(keyedWindow.getStart());
    long endMillis = convertToEpochMilli(keyedWindow.getEnd());
    return String.format(
        "%d:%d:%s",
        startMillis, endMillis, String.join(Constants.DELIMITER, keyedWindow.getKeysList()));
  }

  private static long convertToEpochMilli(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).toEpochMilli();
  }
}
