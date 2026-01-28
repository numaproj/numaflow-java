package io.numaproj.numaflow.shared;

import com.google.protobuf.ByteString;
import common.MetadataOuterClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class UserMetadataTest {

    @Test
    public void testDefaultConstructor() {
        UserMetadata metadata = new UserMetadata();
        assertNotNull(metadata.getGroups());
        assertTrue(metadata.getGroups().isEmpty());

        assertNotNull(metadata.getKeys("missing"));
        assertTrue(metadata.getKeys("missing").isEmpty());

        assertNull(metadata.getValue("missing", "missing"));
    }

    @Test
    public void testProtoConstructor_withNullMetadata() {
        UserMetadata metadata = new UserMetadata((MetadataOuterClass.Metadata) null);
        assertNotNull(metadata.getGroups());
        assertTrue(metadata.getGroups().isEmpty());
    }

    @Test
    public void testProtoConstructor_withEmptyMetadata() {
        MetadataOuterClass.Metadata protoMetadata = MetadataOuterClass.Metadata.newBuilder().build();
        UserMetadata metadata = new UserMetadata(protoMetadata);
        assertNotNull(metadata.getGroups());
        assertTrue(metadata.getGroups().isEmpty());
    }

    @Test
    public void testProtoConstructor_withValidMetadata() {
        MetadataOuterClass.KeyValueGroup kvGroup1 = MetadataOuterClass.KeyValueGroup.newBuilder()
                .putKeyValue("key1", ByteString.copyFromUtf8("value1"))
                .build();

        MetadataOuterClass.KeyValueGroup kvGroup2 = MetadataOuterClass.KeyValueGroup.newBuilder()
                .putKeyValue("keyA", ByteString.copyFromUtf8("valueA"))
                .build();

        MetadataOuterClass.Metadata protoMetadata = MetadataOuterClass.Metadata.newBuilder()
                .putUserMetadata("group1", kvGroup1)
                .putUserMetadata("group2", kvGroup2)
                .build();

        UserMetadata metadata = new UserMetadata(protoMetadata);

        assertEquals(2, metadata.getGroups().size());
        assertArrayEquals("value1".getBytes(), metadata.getValue("group1", "key1"));
        assertArrayEquals("valueA".getBytes(), metadata.getValue("group2", "keyA"));
    }

    @Test
    public void testCopyConstructor_deepCopy() {
        UserMetadata original = new UserMetadata();
        original.addKV("g", "k", "value".getBytes());

        UserMetadata copy = new UserMetadata(original);

        assertArrayEquals("value".getBytes(), copy.getValue("g", "k"));

        byte[] fromCopy = copy.getValue("g", "k");
        fromCopy[0] = 'X';

        assertArrayEquals("value".getBytes(), original.getValue("g", "k"));
        assertArrayEquals("value".getBytes(), copy.getValue("g", "k"));
    }

    @Test
    public void testCopyConstructor_withNullInput() {
        UserMetadata copy = new UserMetadata((UserMetadata) null);
        assertNotNull(copy.getGroups());
        assertTrue(copy.getGroups().isEmpty());
    }

    @Test
    public void testToProto_roundTrip() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("g1", "k1", "v1".getBytes());
        metadata.addKV("g1", "k2", "v2".getBytes());
        metadata.addKV("g2", "ka", "va".getBytes());

        MetadataOuterClass.Metadata proto = metadata.toProto();
        UserMetadata roundTrip = new UserMetadata(proto);

        assertEquals(metadata.getGroups().size(), roundTrip.getGroups().size());
        assertArrayEquals("v1".getBytes(), roundTrip.getValue("g1", "k1"));
        assertArrayEquals("v2".getBytes(), roundTrip.getValue("g1", "k2"));
        assertArrayEquals("va".getBytes(), roundTrip.getValue("g2", "ka"));
    }

    @Test
    public void testAddKV_ignoresNulls() {
        UserMetadata metadata = new UserMetadata();

        metadata.addKV(null, "k", "v".getBytes());
        metadata.addKV("g", null, "v".getBytes());
        metadata.addKV("g", "k", null);

        assertTrue(metadata.getGroups().isEmpty());
    }

    @Test
    public void testAddKV_defensiveCopy() {
        UserMetadata metadata = new UserMetadata();

        byte[] value = "value".getBytes();
        metadata.addKV("g", "k", value);

        value[0] = 'X';

        assertArrayEquals("value".getBytes(), metadata.getValue("g", "k"));
    }

    @Test
    public void testGetValue_returnsClone() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("g", "k", "value".getBytes());

        byte[] got = metadata.getValue("g", "k");
        assertArrayEquals("value".getBytes(), got);

        got[0] = 'X';

        assertArrayEquals("value".getBytes(), metadata.getValue("g", "k"));
    }

    @Test
    public void testAddKVs_ignoresNullGroupOrMap() {
        UserMetadata metadata = new UserMetadata();
        Map<String, byte[]> kv = new HashMap<>();
        kv.put("k", "v".getBytes());

        metadata.addKVs(null, kv);
        metadata.addKVs("g", null);

        assertTrue(metadata.getGroups().isEmpty());
    }

    @Test
    public void testAddKVs_filtersNullValues() {
        UserMetadata metadata = new UserMetadata();

        Map<String, byte[]> kv = new HashMap<>();
        kv.put("k1", "v1".getBytes());
        kv.put("k2", null);

        metadata.addKVs("g", kv);

        List<String> keys = metadata.getKeys("g");
        assertEquals(1, keys.size());
        assertTrue(keys.contains("k1"));
        assertArrayEquals("v1".getBytes(), metadata.getValue("g", "k1"));
        assertNull(metadata.getValue("g", "k2"));
    }

    @Test
    public void testDeleteKeyAndDeleteGroupAndClear() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("g1", "k1", "v1".getBytes());
        metadata.addKV("g2", "k2", "v2".getBytes());

        metadata.deleteKey("g1", "k1");
        assertNull(metadata.getValue("g1", "k1"));

        metadata.deleteKey("missingGroup", "any"); // no-op

        metadata.deleteGroup("g2");
        assertTrue(metadata.getKeys("g2").isEmpty());
        assertNull(metadata.getValue("g2", "k2"));

        metadata.clear();
        assertTrue(metadata.getGroups().isEmpty());
    }
}
