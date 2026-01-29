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
    public void testProtoConstructor() {
        UserMetadata metadata1 = new UserMetadata((MetadataOuterClass.Metadata) null);
        assertNotNull(metadata1.getGroups());
        assertTrue(metadata1.getGroups().isEmpty());

        // test with empty metadata
        MetadataOuterClass.Metadata protoMetadata1 = MetadataOuterClass.Metadata.newBuilder().build();
        UserMetadata metadata2 = new UserMetadata(protoMetadata1);
        assertNotNull(metadata2.getGroups());
        assertTrue(metadata2.getGroups().isEmpty());

        // test with valid metadata
        MetadataOuterClass.KeyValueGroup kvGroup1 = MetadataOuterClass.KeyValueGroup.newBuilder()
                .putKeyValue("key1", ByteString.copyFromUtf8("value1"))
                .build();

        MetadataOuterClass.KeyValueGroup kvGroup2 = MetadataOuterClass.KeyValueGroup.newBuilder()
                .putKeyValue("keyA", ByteString.copyFromUtf8("valueA"))
                .build();

        MetadataOuterClass.Metadata protoMetadata2 = MetadataOuterClass.Metadata.newBuilder()
                .putUserMetadata("group1", kvGroup1)
                .putUserMetadata("group2", kvGroup2)
                .build();

        UserMetadata metadata3 = new UserMetadata(protoMetadata2);

        assertEquals(2, metadata3.getGroups().size());
        assertArrayEquals("value1".getBytes(), metadata3.getValue("group1", "key1"));
        assertArrayEquals("valueA".getBytes(), metadata3.getValue("group2", "keyA"));
    }

    @Test
    public void testCopyConstructor() {
        // Deep copy test
        UserMetadata original = new UserMetadata();
        original.addKV("g", "k", "value".getBytes());

        UserMetadata copy1 = new UserMetadata(original);

        assertArrayEquals("value".getBytes(), copy1.getValue("g", "k"));

        byte[] fromCopy = copy1.getValue("g", "k");
        fromCopy[0] = 'X';

        assertArrayEquals("value".getBytes(), original.getValue("g", "k"));
        assertArrayEquals("value".getBytes(), copy1.getValue("g", "k"));

        // test with null input
        UserMetadata copy2 = new UserMetadata((UserMetadata) null);
        assertNotNull(copy2.getGroups());
        assertTrue(copy2.getGroups().isEmpty());
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
    public void testAddKV() {
        // test with nulls
        UserMetadata metadata1 = new UserMetadata();

        metadata1.addKV(null, "k", "v".getBytes());
        metadata1.addKV("g", null, "v".getBytes());
        metadata1.addKV("g", "k", null);

        assertTrue(metadata1.getGroups().isEmpty());

        // test defensive copy
        UserMetadata metadata2 = new UserMetadata();

        byte[] value = "value".getBytes();
        metadata2.addKV("g", "k", value);

        value[0] = 'X';

        assertArrayEquals("value".getBytes(), metadata2.getValue("g", "k"));
    }

    @Test
    public void testGetValue() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("g", "k", "value".getBytes());

        byte[] got = metadata.getValue("g", "k");
        assertArrayEquals("value".getBytes(), got);

        got[0] = 'X';

        assertArrayEquals("value".getBytes(), metadata.getValue("g", "k"));
    }

    @Test
    public void testAddKVs() {
        // test with nulls
        UserMetadata metadata1 = new UserMetadata();
        Map<String, byte[]> kv1 = new HashMap<>();
        kv1.put("k", "v".getBytes());

        metadata1.addKVs(null, kv1);
        metadata1.addKVs("g", null);

        assertTrue(metadata1.getGroups().isEmpty());

        // test filters null values
        UserMetadata metadata2 = new UserMetadata();

        Map<String, byte[]> kv2 = new HashMap<>();
        kv2.put("k1", "v1".getBytes());
        kv2.put("k2", null);

        metadata2.addKVs("g", kv2);

        List<String> keys1 = metadata2.getKeys("g");
        assertEquals(1, keys1.size());
        assertTrue(keys1.contains("k1"));
        assertArrayEquals("v1".getBytes(), metadata2.getValue("g", "k1"));
        assertNull(metadata2.getValue("g", "k2"));

        // test add to existing group
        UserMetadata metadata3 = new UserMetadata();

        // First, add some initial key-value pairs to a group
        Map<String, byte[]> initialKv = new HashMap<>();
        initialKv.put("k1", "v1".getBytes());
        metadata3.addKVs("g", initialKv);

        // Verify initial state
        assertEquals(1, metadata3.getKeys("g").size());
        assertArrayEquals("v1".getBytes(), metadata3.getValue("g", "k1"));

        // Now add more key-value pairs to the same existing group
        Map<String, byte[]> additionalKv = new HashMap<>();
        additionalKv.put("k2", "v2".getBytes());
        additionalKv.put("k3", "v3".getBytes());
        metadata3.addKVs("g", additionalKv);

        // Verify all keys are present in the group
        List<String> keys2 = metadata3.getKeys("g");
        assertEquals(3, keys2.size());
        assertTrue(keys2.contains("k1"));
        assertTrue(keys2.contains("k2"));
        assertTrue(keys2.contains("k3"));

        // Verify all values
        assertArrayEquals("v1".getBytes(), metadata3.getValue("g", "k1"));
        assertArrayEquals("v2".getBytes(), metadata3.getValue("g", "k2"));
        assertArrayEquals("v3".getBytes(), metadata3.getValue("g", "k3"));
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
