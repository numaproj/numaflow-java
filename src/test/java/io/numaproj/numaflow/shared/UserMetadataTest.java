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
        assertNotNull(metadata.getData());
        assertTrue(metadata.getData().isEmpty());
    }

    @Test
    public void testMapConstructor_withValidData() {
        Map<String, Map<String, byte[]>> data = new HashMap<>();
        Map<String, byte[]> group1 = new HashMap<>();
        group1.put("key1", "value1".getBytes());
        group1.put("key2", "value2".getBytes());
        data.put("group1", group1);

        UserMetadata metadata = new UserMetadata(data);

        assertNotNull(metadata.getData());
        assertEquals(1, metadata.getData().size());
        assertTrue(metadata.getData().containsKey("group1"));
        assertArrayEquals("value1".getBytes(), metadata.getData().get("group1").get("key1"));
        assertArrayEquals("value2".getBytes(), metadata.getData().get("group1").get("key2"));
    }

    @Test
    public void testMapConstructor_withNullData() {
        UserMetadata metadata = new UserMetadata((Map<String, Map<String, byte[]>>) null);
        assertNotNull(metadata.getData());
        assertTrue(metadata.getData().isEmpty());
    }

    @Test
    public void testMapConstructor_withEmptyData() {
        Map<String, Map<String, byte[]>> data = new HashMap<>();
        UserMetadata metadata = new UserMetadata(data);
        assertNotNull(metadata.getData());
        assertTrue(metadata.getData().isEmpty());
    }

    @Test
    public void testMapConstructor_filtersNullValues() {
        Map<String, Map<String, byte[]>> data = new HashMap<>();
        Map<String, byte[]> group1 = new HashMap<>();
        group1.put("key1", "value1".getBytes());
        group1.put("key2", null);
        data.put("group1", group1);
        data.put("group2", null);

        UserMetadata metadata = new UserMetadata(data);

        assertEquals(1, metadata.getData().size());
        assertEquals(1, metadata.getData().get("group1").size());
        assertArrayEquals("value1".getBytes(), metadata.getData().get("group1").get("key1"));
        assertNull(metadata.getData().get("group1").get("key2"));
    }

    @Test
    public void testMapConstructor_preventsMutation() {
        Map<String, Map<String, byte[]>> data = new HashMap<>();
        Map<String, byte[]> group1 = new HashMap<>();
        byte[] originalValue = "value1".getBytes();
        group1.put("key1", originalValue);
        data.put("group1", group1);

        UserMetadata metadata = new UserMetadata(data);

        // Modify original byte array
        originalValue[0] = 'X';

        // Verify metadata is not affected
        assertArrayEquals("value1".getBytes(), metadata.getData().get("group1").get("key1"));
    }

    @Test
    public void testProtoConstructor_withValidMetadata() {
        MetadataOuterClass.KeyValueGroup kvGroup = MetadataOuterClass.KeyValueGroup.newBuilder()
                .putKeyValue("key1", ByteString.copyFromUtf8("value1"))
                .putKeyValue("key2", ByteString.copyFromUtf8("value2"))
                .build();

        MetadataOuterClass.Metadata protoMetadata = MetadataOuterClass.Metadata.newBuilder()
                .putUserMetadata("group1", kvGroup)
                .build();

        UserMetadata metadata = new UserMetadata(protoMetadata);

        assertNotNull(metadata.getData());
        assertEquals(1, metadata.getData().size());
        assertTrue(metadata.getData().containsKey("group1"));
        assertArrayEquals("value1".getBytes(), metadata.getData().get("group1").get("key1"));
        assertArrayEquals("value2".getBytes(), metadata.getData().get("group1").get("key2"));
    }

    @Test
    public void testProtoConstructor_withNullMetadata() {
        UserMetadata metadata = new UserMetadata((MetadataOuterClass.Metadata) null);
        assertNotNull(metadata.getData());
        assertTrue(metadata.getData().isEmpty());
    }

    @Test
    public void testProtoConstructor_withEmptyMetadata() {
        MetadataOuterClass.Metadata protoMetadata = MetadataOuterClass.Metadata.newBuilder().build();
        UserMetadata metadata = new UserMetadata(protoMetadata);
        assertNotNull(metadata.getData());
        assertTrue(metadata.getData().isEmpty());
    }

    @Test
    public void testCopyConstructor_withValidUserMetadata() {
        UserMetadata original = new UserMetadata();
        original.addKV("group1", "key1", "value1".getBytes());
        original.addKV("group1", "key2", "value2".getBytes());
        original.addKV("group2", "keyA", "valueA".getBytes());
        original.addKV("group2", "keyB", null);

        UserMetadata copy = new UserMetadata(original);

        assertNotNull(copy.getData());
        assertEquals(2, copy.getData().size());
        assertArrayEquals("value1".getBytes(), copy.getData().get("group1").get("key1"));
        assertArrayEquals("value2".getBytes(), copy.getData().get("group1").get("key2"));
        assertArrayEquals("valueA".getBytes(), copy.getData().get("group2").get("keyA"));
        assertNull(copy.getData().get("group2").get("keyB"));
    }

    @Test
    public void testCopyConstructor_withNullUserMetadata() {
        UserMetadata metadata = new UserMetadata((UserMetadata) null);
        assertNotNull(metadata.getData());
        assertTrue(metadata.getData().isEmpty());
    }

    @Test
    public void testCopyConstructor_preventsMutation() {
        UserMetadata original = new UserMetadata();
        byte[] originalValue = "value1".getBytes();
        original.addKV("group1", "key1", originalValue);

        UserMetadata copy = new UserMetadata(original);

        // Modify original
        original.addKV("group1", "key2", "value2".getBytes());

        // Verify copy is not affected
        assertEquals(1, copy.getData().get("group1").size());
        assertNull(copy.getData().get("group1").get("key2"));
    }

    @Test
    public void testToProto() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("group1", "key1", "value1".getBytes());
        metadata.addKV("group1", "key2", "value2".getBytes());
        metadata.addKV("group2", "keyA", "valueA".getBytes());
        metadata.addKV("group2", "keyB", null);

        MetadataOuterClass.Metadata proto = metadata.toProto();

        assertNotNull(proto);
        assertEquals(2, proto.getUserMetadataMap().size());
        assertTrue(proto.getUserMetadataMap().containsKey("group1"));
        assertTrue(proto.getUserMetadataMap().containsKey("group2"));

        MetadataOuterClass.KeyValueGroup group1 = proto.getUserMetadataMap().get("group1");
        assertEquals(ByteString.copyFromUtf8("value1"), group1.getKeyValueMap().get("key1"));
        assertEquals(ByteString.copyFromUtf8("value2"), group1.getKeyValueMap().get("key2"));

        MetadataOuterClass.KeyValueGroup group2 = proto.getUserMetadataMap().get("group2");
        assertEquals(ByteString.copyFromUtf8("valueA"), group2.getKeyValueMap().get("keyA"));
        assertNull(group2.getKeyValueMap().get("keyB"));
    }

    @Test
    public void testToProto_emptyMetadata() {
        UserMetadata metadata = new UserMetadata();
        MetadataOuterClass.Metadata proto = metadata.toProto();

        assertNotNull(proto);
        assertTrue(proto.getUserMetadataMap().isEmpty());
    }

    @Test
    public void testGetData_returnsDeepCopy() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("group1", "key1", "value1".getBytes());

        Map<String, Map<String, byte[]>> dataCopy = metadata.getData();

        // Modify the returned copy
        dataCopy.put("group2", new HashMap<>());
        dataCopy.get("group1").put("key2", "value2".getBytes());
        dataCopy.get("group1").get("key1")[0] = 'X';

        // Verify original metadata is not affected
        assertEquals(1, metadata.getGroups().size());
        assertEquals(1, metadata.getKeys("group1").size());
        assertArrayEquals("value1".getBytes(), metadata.getValue("group1", "key1"));
    }

    @Test
    public void testGetData_emptyMetadata() {
        UserMetadata metadata = new UserMetadata();
        Map<String, Map<String, byte[]>> data = metadata.getData();

        assertNotNull(data);
        assertTrue(data.isEmpty());
    }

    @Test
    public void testGetGroups() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("group1", "key1", "value1".getBytes());
        metadata.addKV("group2", "key2", "value2".getBytes());
        metadata.addKV("group3", "key3", "value3".getBytes());

        List<String> groups = metadata.getGroups();

        assertNotNull(groups);
        assertEquals(3, groups.size());
        assertTrue(groups.contains("group1"));
        assertTrue(groups.contains("group2"));
        assertTrue(groups.contains("group3"));
    }

    @Test
    public void testGetGroups_emptyMetadata() {
        UserMetadata metadata = new UserMetadata();
        List<String> groups = metadata.getGroups();

        assertNotNull(groups);
        assertTrue(groups.isEmpty());
    }

    @Test
    public void testDeleteGroup() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("group1", "key1", "value1".getBytes());
        metadata.addKV("group2", "key2", "value2".getBytes());

        metadata.deleteGroup("group1");

        assertEquals(1, metadata.getData().size());
        assertFalse(metadata.getData().containsKey("group1"));
        assertTrue(metadata.getData().containsKey("group2"));
    }

    @Test
    public void testDeleteGroup_nonExistentGroup() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("group1", "key1", "value1".getBytes());

        // Should not throw exception
        metadata.deleteGroup("nonExistent");

        assertEquals(1, metadata.getData().size());
    }

    @Test
    public void testGetKeys() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("group1", "key1", "value1".getBytes());
        metadata.addKV("group1", "key2", "value2".getBytes());
        metadata.addKV("group1", "key3", "value3".getBytes());

        List<String> keys = metadata.getKeys("group1");

        assertNotNull(keys);
        assertEquals(3, keys.size());
        assertTrue(keys.contains("key1"));
        assertTrue(keys.contains("key2"));
        assertTrue(keys.contains("key3"));
    }

    @Test
    public void testGetKeys_nonExistentGroup() {
        UserMetadata metadata = new UserMetadata();

        List<String> keys = metadata.getKeys("nonExistent");

        assertNotNull(keys);
        assertTrue(keys.isEmpty());
    }

    @Test
    public void testDeleteKey() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("group1", "key1", "value1".getBytes());
        metadata.addKV("group1", "key2", "value2".getBytes());

        metadata.deleteKey("group1", "key1");

        assertEquals(1, metadata.getData().get("group1").size());
        assertNull(metadata.getData().get("group1").get("key1"));
        assertNotNull(metadata.getData().get("group1").get("key2"));
    }

    @Test
    public void testDeleteKey_nonExistentGroup() {
        UserMetadata metadata = new UserMetadata();

        // Should not throw exception
        metadata.deleteKey("nonExistent", "key1");

        assertTrue(metadata.getData().isEmpty());
    }

    @Test
    public void testDeleteKey_nonExistentKey() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("group1", "key1", "value1".getBytes());

        // Should not throw exception
        metadata.deleteKey("group1", "nonExistent");

        assertEquals(1, metadata.getData().get("group1").size());
    }

    @Test
    public void testAddKV() {
        UserMetadata metadata = new UserMetadata();

        metadata.addKV("group1", "key1", "value1".getBytes());

        assertEquals(1, metadata.getData().size());
        assertArrayEquals("value1".getBytes(), metadata.getData().get("group1").get("key1"));
    }

    @Test
    public void testAddKV_nullValue() {
        UserMetadata metadata = new UserMetadata();

        metadata.addKV("group1", "key1", null);
        metadata.addKV(null, "key1", "value1".getBytes());
        metadata.addKV("group1", null, "value1".getBytes());

        assertTrue(metadata.getData().isEmpty());
    }

    @Test
    public void testAddKV_preventsMutation() {
        UserMetadata metadata = new UserMetadata();
        byte[] value = "value1".getBytes();

        metadata.addKV("group1", "key1", value);

        // Modify original byte array
        value[0] = 'X';

        // Verify metadata is not affected
        assertArrayEquals("value1".getBytes(), metadata.getData().get("group1").get("key1"));
    }

    @Test
    public void testAddKV_overwritesExistingKey() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("group1", "key1", "value1".getBytes());

        metadata.addKV("group1", "key1", "newValue".getBytes());

        assertArrayEquals("newValue".getBytes(), metadata.getData().get("group1").get("key1"));
    }

    @Test
    public void testAddKVs() {
        UserMetadata metadata = new UserMetadata();
        Map<String, byte[]> kv = new HashMap<>();
        kv.put("key1", "value1".getBytes());
        kv.put("key2", "value2".getBytes());

        metadata.addKVs("group1", kv);

        assertEquals(1, metadata.getData().size());
        assertEquals(2, metadata.getData().get("group1").size());
        assertArrayEquals("value1".getBytes(), metadata.getData().get("group1").get("key1"));
        assertArrayEquals("value2".getBytes(), metadata.getData().get("group1").get("key2"));
    }

    @Test
    public void testAddKVs_filtersNullValues() {
        UserMetadata metadata = new UserMetadata();
        Map<String, byte[]> kv = new HashMap<>();
        kv.put("key1", "value1".getBytes());
        kv.put("key2", null);

        metadata.addKVs("group1", kv);
        metadata.addKVs(null, kv);
        metadata.addKVs("group1", null);

        assertEquals(1, metadata.getData().get("group1").size());
        assertArrayEquals("value1".getBytes(), metadata.getData().get("group1").get("key1"));
        assertNull(metadata.getData().get("group1").get("key2"));
    }

    @Test
    public void testAddKVs_preventsMutation() {
        UserMetadata metadata = new UserMetadata();
        byte[] value = "value1".getBytes();
        Map<String, byte[]> kv = new HashMap<>();
        kv.put("key1", value);

        metadata.addKVs("group1", kv);

        // Modify original byte array
        value[0] = 'X';

        // Verify metadata is not affected
        assertArrayEquals("value1".getBytes(), metadata.getData().get("group1").get("key1"));
    }

    @Test
    public void testGetValue() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("group1", "key1", "value1".getBytes());

        byte[] value = metadata.getValue("group1", "key1");

        assertNotNull(value);
        assertArrayEquals("value1".getBytes(), value);
    }

    @Test
    public void testGetValue_nonExistentGroup() {
        UserMetadata metadata = new UserMetadata();

        byte[] value = metadata.getValue("nonExistent", "key1");

        assertNull(value);
    }

    @Test
    public void testGetValue_nonExistentKey() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("group1", "key1", "value1".getBytes());

        byte[] value = metadata.getValue("group1", "nonExistent");

        assertNull(value);
    }

    @Test
    public void testGetValue_returnsClone() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("group1", "key1", "value1".getBytes());

        byte[] value = metadata.getValue("group1", "key1");
        value[0] = 'X';

        // Verify internal data is not affected
        assertArrayEquals("value1".getBytes(), metadata.getValue("group1", "key1"));
    }

    @Test
    public void testClear() {
        UserMetadata metadata = new UserMetadata();
        metadata.addKV("group1", "key1", "value1".getBytes());
        metadata.addKV("group2", "key2", "value2".getBytes());

        metadata.clear();

        assertNotNull(metadata.getData());
        assertTrue(metadata.getData().isEmpty());
    }

    @Test
    public void testClear_emptyMetadata() {
        UserMetadata metadata = new UserMetadata();

        // Should not throw exception
        metadata.clear();

        assertTrue(metadata.getData().isEmpty());
    }

    @Test
    public void testRoundTrip_toProtoAndBack() {
        UserMetadata original = new UserMetadata();
        original.addKV("group1", "key1", "value1".getBytes());
        original.addKV("group1", "key2", "value2".getBytes());
        original.addKV("group2", "keyA", "valueA".getBytes());

        MetadataOuterClass.Metadata proto = original.toProto();
        UserMetadata reconstructed = new UserMetadata(proto);

        assertEquals(original.getData().size(), reconstructed.getData().size());
        assertArrayEquals(
                original.getValue("group1", "key1"),
                reconstructed.getValue("group1", "key1"));
        assertArrayEquals(
                original.getValue("group1", "key2"),
                reconstructed.getValue("group1", "key2"));
        assertArrayEquals(
                original.getValue("group2", "keyA"),
                reconstructed.getValue("group2", "keyA"));
    }
}
