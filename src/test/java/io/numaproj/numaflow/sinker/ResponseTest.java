package io.numaproj.numaflow.sinker;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import common.MetadataOuterClass;
import io.numaproj.numaflow.sink.v1.SinkOuterClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.util.Map.entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ResponseTest {
    @Test
    public void test_addResponse() {
        String defaultId = "id";
        Response response1 = Response.responseFallback(defaultId);
        assertEquals(defaultId, response1.getId());
        Response response2 = Response.responseOK(defaultId);
        assertEquals(defaultId, response2.getId());
        Response response3 = Response.responseServe(defaultId, "serve".getBytes());
        assertEquals(defaultId, response3.getId());
        Response response4 = Response.responseFailure(defaultId, "failure");
        assertEquals(defaultId, response4.getId());

        HashMap<String, KeyValueGroup> userMetadata = new HashMap<>();
        userMetadata.put("group1", KeyValueGroup.builder().build());
        HashMap<String, byte[]> kvg1 = new HashMap<>(Map.ofEntries(
                entry("key1", "val1".getBytes())
        ));
        kvg1.put("key2", null);

        userMetadata.put("group2", KeyValueGroup.builder().keyValue(kvg1).build());
        userMetadata.put("group3", null);
        OnSuccessMessage onSuccessMessage1 = new OnSuccessMessage("onSuccessValue".getBytes(), null, userMetadata);

        Response response5 = Response.responseOnSuccess(defaultId, onSuccessMessage1);
        assertEquals(defaultId, response5.getId());
        assertEquals("", response5.getOnSuccessMessage().getKeys(0));

        OnSuccessMessage onSuccessMessage2 = new OnSuccessMessage("onSuccessValue".getBytes(), null, null);
        Response response6 = Response.responseOnSuccess(defaultId, onSuccessMessage2);
        assertEquals(defaultId, response6.getId());
        assertEquals(MetadataOuterClass.Metadata.newBuilder()
                        .putAllUserMetadata(MetadataOuterClass.Metadata
                        .getDefaultInstance()
                        .getUserMetadataMap()).build(),
                response6.getOnSuccessMessage().getMetadata());

        OnSuccessMessage onSuccessMessage3 = new OnSuccessMessage(null, "key", null);
        Response response7 = Response.responseOnSuccess(defaultId, onSuccessMessage3);
        assertEquals(defaultId, response7.getId());
        assertEquals(ByteString.copyFrom("".getBytes()), response7.getOnSuccessMessage().getValue());
        assertEquals("key", response7.getOnSuccessMessage().getKeys(0));

        Response response8 = Response.responseOnSuccess(defaultId, (OnSuccessMessage) null);
        assertEquals(defaultId, response8.getId());
        assertNull(response8.getOnSuccessMessage());

        Response response9 = Response.responseOnSuccess(defaultId, ( SinkOuterClass.SinkResponse.Result.Message) null);
        assertNull(response9.getOnSuccessMessage());
    }
}
