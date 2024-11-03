package io.numaproj.numaflow.examples.sink.simple;

import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.SinkerTestKit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


@Slf4j
public class SimpleSinkTest {

    @Test
    public void testSimpleSink() {
        /*
        int datumCount = 10;
        SimpleSink simpleSink = new SimpleSink();

        // Create a test datum iterator with 10 messages
        SinkerTestKit.TestListIterator testListIterator = new SinkerTestKit.TestListIterator();
        for (int i = 0; i < datumCount; i++) {
            testListIterator.addDatum(SinkerTestKit.TestDatum
                    .builder()
                    .id("id-" + i)
                    .value(("value-" + i).getBytes())
                    .build());
        }

        ResponseList responseList = simpleSink.processMessages(testListIterator);
        Assertions.assertEquals(datumCount, responseList.getResponses().size());
        for (Response response : responseList.getResponses()) {
            Assertions.assertEquals(true, response.getSuccess());
        }
        // we can add the logic to verify if the messages were
        // successfully written to the sink(could be a file, database, etc.)

         */
    }
}
