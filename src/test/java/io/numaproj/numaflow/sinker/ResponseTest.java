package io.numaproj.numaflow.sinker;

import com.google.protobuf.ByteString;
import common.MetadataOuterClass;
import io.numaproj.numaflow.shared.UserMetadata;
import io.numaproj.numaflow.sink.v1.SinkOuterClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Map.entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ResponseTest {
    @Test
    public void test_addResponse() {
        String defaultId = "id";
        // test fallback response creation
        Response response1 = Response.responseFallback(defaultId);
        assertEquals(defaultId, response1.getId());
        // test ok response creation
        Response response2 = Response.responseOK(defaultId);
        assertEquals(defaultId, response2.getId());
        // test serve response creation
        Response response3 = Response.responseServe(defaultId, "serve".getBytes());
        assertEquals(defaultId, response3.getId());
        // test failure response creation
        Response response4 = Response.responseFailure(defaultId, "failure");
        assertEquals(defaultId, response4.getId());

        // test onSuccess response creation with on success message containing user metadata and no keys
        HashMap<String, byte[]> kvg1 = new HashMap<>(Map.ofEntries(
                entry("key1", "val1".getBytes())
        ));
        kvg1.put("key2", null);

        UserMetadata userMetadata = new UserMetadata();
        userMetadata.addKV("group1", "key2", null);
        userMetadata.addKVs("group2", kvg1);
        userMetadata.addKVs("group3", null);
        Message onSuccessMessage1 = new Message("onSuccessValue".getBytes(), null, new UserMetadata(userMetadata));

        Response response5 = Response.responseOnSuccess(defaultId, onSuccessMessage1);
        assertEquals(defaultId, response5.getId());
        assertNull(response5.getOnSuccessMessage().getKeys());
        assertEquals("onSuccessValue", new String(response5.getOnSuccessMessage().getValue(), StandardCharsets.UTF_8));
        assertEquals(userMetadata.toProto(), response5.getOnSuccessMessage().getUserMetadata().toProto());

        // test onSuccess response creation with on success message containing no user metadata and no keys
        Message onSuccessMessage2 = new Message("onSuccessValue".getBytes(), null, null);
        Response response6 = Response.responseOnSuccess(defaultId, onSuccessMessage2);
        assertEquals(defaultId, response6.getId());
        assertNull(response6.getOnSuccessMessage().getUserMetadata());

        // test onSuccess response creation with on success message containing keys but no user metadata or value
        Message onSuccessMessage3 = new Message(null, new String[] {"key"}, null);
        Response response7 = Response.responseOnSuccess(defaultId, onSuccessMessage3);
        assertEquals(defaultId, response7.getId());
        assertNull(response7.getOnSuccessMessage().getValue());
        assertEquals("key", response7.getOnSuccessMessage().getKeys()[0]);

        // test onSuccess response creation with no success message
        Response response8 = Response.responseOnSuccess(defaultId, null);
        assertEquals(defaultId, response8.getId());
        assertNull(response8.getOnSuccessMessage());

        //
        Response response9 = Response.responseOnSuccess(defaultId, null);
        assertNull(response9.getOnSuccessMessage());
    }
}
