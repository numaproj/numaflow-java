package io.numaproj.numaflow.examples.function.map.eventtimefilter;

import io.numaproj.numaflow.function.Datum;
import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.MessageT;
import io.numaproj.numaflow.function.mapt.MapTHandler;

import java.io.IOException;
import java.time.Instant;

/**
 * This is a simple User Defined Function example which receives a message, applies the following
 * data transformation, and returns the message.
 * <p>
 * If the message event time is before year 2022, drop the message. If it's within year 2022, update
 * the key to "within_year_2022" and update the message event time to Jan 1st 2022.
 * Otherwise, (exclusively after year 2022), update the key to "after_year_2022" and update the
 * message event time to Jan 1st 2023.
 */
public class EventTimeFilterFunction extends MapTHandler {

    private static final Instant januaryFirst2022 = Instant.ofEpochMilli(1640995200000L);
    private static final Instant januaryFirst2023 = Instant.ofEpochMilli(1672531200000L);

    public MessageT[] processMessage(String[] key, Datum data) {
        Instant eventTime = data.getEventTime();

        if (eventTime.isBefore(januaryFirst2022)) {
            return new MessageT[]{MessageT.toDrop()};
        } else if (eventTime.isBefore(januaryFirst2023)) {
            return new MessageT[]{
                    MessageT.to(
                            januaryFirst2022,
                            new String[]{"within_year_2022"},
                            data.getValue())};
        } else {
            return new MessageT[]{
                    MessageT.to(
                            januaryFirst2023,
                            new String[]{"after_year_2022"},
                            data.getValue())};
        }
    }

    public static void main(String[] args) throws IOException {
        new FunctionServer()
                .registerMapTHandler(new EventTimeFilterFunction())
                .start();
    }
}
