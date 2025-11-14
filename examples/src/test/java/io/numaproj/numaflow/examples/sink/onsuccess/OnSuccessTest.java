package io.numaproj.numaflow.examples.sink.onsuccess;

import io.numaproj.numaflow.examples.sink.simple.SimpleSink;
import io.numaproj.numaflow.sinker.Response;
import io.numaproj.numaflow.sinker.ResponseList;
import io.numaproj.numaflow.sinker.SinkerTestKit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OnSuccessTest {
    @Test
    public void testOnSuccessSink() {
        int datumCount = 10;
        OnSuccess onSuccessSink = new OnSuccess();
        // Create a test datum iterator with 10 messages
        SinkerTestKit.TestListIterator testListIterator = new SinkerTestKit.TestListIterator();
        for (int i = 0; i < datumCount; i++) {
            testListIterator.addDatum(
                    SinkerTestKit.TestDatum
                            .builder()
                            .id("id-" + i)
                            .value(("value-" + i).getBytes())
                            .build());
        }
        ResponseList responseList = onSuccessSink.processMessages(testListIterator);
        Assertions.assertEquals(datumCount, responseList.getResponses().size());
        for (Response response : responseList.getResponses()) {
            Assertions.assertEquals(false, response.getSuccess());
        }
        // we can add the logic to verify if the messages were
        // successfully written to the sink(could be a file, database, etc.)
    }
}
