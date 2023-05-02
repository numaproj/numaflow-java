package io.numaproj.numaflow.examples.function.map.eventtimefilter;

import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.handlers.MapTHandler;
import io.numaproj.numaflow.function.interfaces.Datum;
import io.numaproj.numaflow.function.types.MessageT;
import io.numaproj.numaflow.function.types.MessageTList;

import java.time.Instant;

/**
 * This is a simple User Defined Function example which receives a message, applies the following
 * data transformation, and returns the message.
 * <p>
 * If the message event time is before year 2022, drop the message. If it's within year 2022, update
 * the tag to "within_year_2022" and update the message event time to Jan 1st 2022.
 * Otherwise, (exclusively after year 2022), update the tag to "after_year_2022" and update the
 * message event time to Jan 1st 2023.
 */
public class EventTimeFilterFunction extends MapTHandler {

    private static final Instant januaryFirst2022 = Instant.ofEpochMilli(1640995200000L);
    private static final Instant januaryFirst2023 = Instant.ofEpochMilli(1672531200000L);

    public static void main(String[] args) throws Exception {
        new FunctionServer()
                .registerMapTHandler(new EventTimeFilterFunction())
                .start();
    }

    public MessageTList processMessage(String[] keys, Datum data) {
        Instant eventTime = data.getEventTime();

        if (eventTime.isBefore(januaryFirst2022)) {
            return MessageTList.newBuilder().addMessage(MessageT.toDrop()).build();
        } else if (eventTime.isBefore(januaryFirst2023)) {
            return MessageTList
                    .newBuilder()
                    .addMessage(
                            new MessageT(
                                    data.getValue(),
                                    januaryFirst2022,
                                    null,
                                    new String[]{"within_year_2022"}))
                    .build();
        } else {
            return MessageTList
                    .newBuilder()
                    .addMessage(new MessageT(
                            data.getValue(),
                            januaryFirst2023,
                            null,
                            new String[]{"after_year_2022"}))
                    .build();
        }
    }
}
