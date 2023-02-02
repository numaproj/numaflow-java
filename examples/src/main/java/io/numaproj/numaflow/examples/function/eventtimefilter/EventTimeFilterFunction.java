package io.numaproj.numaflow.examples.function.eventtimefilter;

import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.MessageT;
import io.numaproj.numaflow.function.mapt.MapTFunc;
import io.numaproj.numaflow.function.v1.Udfunction;

import java.io.IOException;
import java.time.Instant;
import java.util.logging.Logger;

/**
 * This is a simple User Defined Function example which receives a message, applies the following
 * data transformation, and returns the message.
 * <p>
 * If the message event time is before year 2022, drop the message. If it's within year 2022, update
 * the key to "within_year_2022" and update the message event time to Jan 1st 2022.
 * Otherwise, (exclusively after year 2022), update the key to "after_year_2022" and update the
 * message event time to Jan 1st 2023.
 */
public class EventTimeFilterFunction {

    private static final Logger logger = Logger.getLogger(EventTimeFilterFunction.class.getName());
    private static final Instant januaryFirst2022 = Instant.ofEpochMilli(1640995200000L);
    private static final Instant januaryFirst2023 = Instant.ofEpochMilli(1672531200000L);

    private static MessageT[] process(String key, Udfunction.Datum data) {
        Instant eventTime = Instant.ofEpochSecond(
                data.getEventTime().getEventTime().getSeconds(),
                data.getEventTime().getEventTime().getNanos());

        if (eventTime.isBefore(januaryFirst2022)) {
            return new MessageT[]{MessageT.toDrop()};
        } else if (eventTime.isBefore(januaryFirst2023)) {
            return new MessageT[]{
                    MessageT.to(
                            januaryFirst2022,
                            "within_year_2022",
                            data.getValue().toByteArray())};
        } else {
            return new MessageT[]{
                    MessageT.to(
                            januaryFirst2023,
                            "after_year_2022",
                            data.getValue().toByteArray())};
        }
    }

    public static void main(String[] args) throws IOException {
        new FunctionServer()
                .registerMapperT(new MapTFunc(EventTimeFilterFunction::process))
                .start();
    }
}
