package io.numaproj.numaflow.shared;

import com.google.protobuf.ByteString;
import common.MetadataOuterClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class SystemMetadataTest {

    @Test
    public void testDefaultConstructor() {
        SystemMetadata metadata = new SystemMetadata();
        assertNotNull(metadata.getData());
        assertTrue(metadata.getData().isEmpty());
    }

    @Test
    public void testMapConstructor_withValidData() {
        Map<String, Map<String, byte[]>> data = new HashMap<>();
        Map<String, byte[]> group1 = new HashMap<>();
        group1.put("key1", "value1".getBytes());
        Map<String, byte[]> group2 = new HashMap<>();
        group2.put("keyA", "valueA".getBytes());
        data.put("group1", group1);
        data.put("group2", group2);

        SystemMetadata metadata = new SystemMetadata(data);

        assertEquals(2, metadata.getData().size());
        assertArrayEquals("value1".getBytes(), metadata.getData().get("group1").get("key1"));
        assertArrayEquals("valueA".getBytes(), metadata.getData().get("group2").get("keyA"));
    }

    @Test
    public void testMapConstructor_withNullData() {
        SystemMetadata metadata = new SystemMetadata((Map<String, Map<String, byte[]>>) null);
        assertNotNull(metadata.getData());
        assertTrue(metadata.getData().isEmpty());
    }

    @Test
    public void testMapConstructor_withEmptyData() {
        Map<String, Map<String, byte[]>> data = new HashMap<>();
        SystemMetadata metadata = new SystemMetadata(data);
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

        SystemMetadata metadata = new SystemMetadata(data);

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

        SystemMetadata metadata = new SystemMetadata(data);

        // Modify original byte array
        originalValue[0] = 'X';

        // Verify metadata is not affected
        assertArrayEquals("value1".getBytes(), metadata.getData().get("group1").get("key1"));
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
                .putSysMetadata("group1", kvGroup1)
                .putSysMetadata("group2", kvGroup2)
                .build();

        SystemMetadata metadata = new SystemMetadata(protoMetadata);

        assertEquals(2, metadata.getData().size());
        assertArrayEquals("value1".getBytes(), metadata.getData().get("group1").get("key1"));
        assertArrayEquals("valueA".getBytes(), metadata.getData().get("group2").get("keyA"));
    }

    @Test
    public void testProtoConstructor_withNullMetadata() {
        SystemMetadata metadata = new SystemMetadata((MetadataOuterClass.Metadata) null);
        assertNotNull(metadata.getData());
        assertTrue(metadata.getData().isEmpty());
    }

    @Test
    public void testProtoConstructor_withEmptyMetadata() {
        MetadataOuterClass.Metadata protoMetadata = MetadataOuterClass.Metadata.newBuilder().build();
        SystemMetadata metadata = new SystemMetadata(protoMetadata);
        assertNotNull(metadata.getData());
        assertTrue(metadata.getData().isEmpty());
    }

    @Test
    public void testGetData_returnsDeepCopy() {
        Map<String, Map<String, byte[]>> inputData = new HashMap<>();
        Map<String, byte[]> group1 = new HashMap<>();
        group1.put("key1", "value1".getBytes());
        inputData.put("group1", group1);

        SystemMetadata metadata = new SystemMetadata(inputData);
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
        SystemMetadata metadata = new SystemMetadata();
        Map<String, Map<String, byte[]>> data = metadata.getData();

        assertNotNull(data);
        assertTrue(data.isEmpty());
    }

    @Test
    public void testGetGroups() {
        Map<String, Map<String, byte[]>> data = new HashMap<>();
        data.put("group1", new HashMap<>());
        data.put("group2", new HashMap<>());
        data.put("group3", new HashMap<>());

        SystemMetadata metadata = new SystemMetadata(data);
        List<String> groups = metadata.getGroups();

        assertNotNull(groups);
        assertEquals(3, groups.size());
        assertTrue(groups.contains("group1"));
        assertTrue(groups.contains("group2"));
        assertTrue(groups.contains("group3"));
    }

    @Test
    public void testGetGroups_emptyMetadata() {
        SystemMetadata metadata = new SystemMetadata();
        List<String> groups = metadata.getGroups();

        assertNotNull(groups);
        assertTrue(groups.isEmpty());
    }

    @Test
    public void testGetKeys() {
        Map<String, Map<String, byte[]>> data = new HashMap<>();
        Map<String, byte[]> group1 = new HashMap<>();
        group1.put("key1", "value1".getBytes());
        group1.put("key2", "value2".getBytes());
        group1.put("key3", "value3".getBytes());
        data.put("group1", group1);

        SystemMetadata metadata = new SystemMetadata(data);
        List<String> keys = metadata.getKeys("group1");

        assertNotNull(keys);
        assertEquals(3, keys.size());
        assertTrue(keys.contains("key1"));
        assertTrue(keys.contains("key2"));
        assertTrue(keys.contains("key3"));
    }

    @Test
    public void testGetKeys_nonExistentGroup() {
        SystemMetadata metadata = new SystemMetadata();
        List<String> keys = metadata.getKeys("nonExistent");

        assertNotNull(keys);
        assertTrue(keys.isEmpty());
    }

    @Test
    public void testGetKeys_emptyGroup() {
        Map<String, Map<String, byte[]>> data = new HashMap<>();
        data.put("emptyGroup", new HashMap<>());

        SystemMetadata metadata = new SystemMetadata(data);
        List<String> keys = metadata.getKeys("emptyGroup");

        assertNotNull(keys);
        assertTrue(keys.isEmpty());
    }

    @Test
    public void testGetValue() {
        Map<String, Map<String, byte[]>> data = new HashMap<>();
        Map<String, byte[]> group1 = new HashMap<>();
        group1.put("key1", "value1".getBytes());
        data.put("group1", group1);

        SystemMetadata metadata = new SystemMetadata(data);
        byte[] value = metadata.getValue("group1", "key1");

        assertNotNull(value);
        assertArrayEquals("value1".getBytes(), value);
    }

    @Test
    public void testGetValue_nonExistentGroup() {
        SystemMetadata metadata = new SystemMetadata();
        byte[] value = metadata.getValue("nonExistent", "key1");

        assertNull(value);
    }

    @Test
    public void testGetValue_nonExistentKey() {
        Map<String, Map<String, byte[]>> data = new HashMap<>();
        Map<String, byte[]> group1 = new HashMap<>();
        group1.put("key1", "value1".getBytes());
        data.put("group1", group1);

        SystemMetadata metadata = new SystemMetadata(data);
        byte[] value = metadata.getValue("group1", "nonExistent");

        assertNull(value);
    }

    @Test
    public void testGetValue_returnsClone() {
        Map<String, Map<String, byte[]>> data = new HashMap<>();
        Map<String, byte[]> group1 = new HashMap<>();
        group1.put("key1", "value1".getBytes());
        data.put("group1", group1);

        SystemMetadata metadata = new SystemMetadata(data);
        byte[] value = metadata.getValue("group1", "key1");
        value[0] = 'X';

        // Verify internal data is not affected
        assertArrayEquals("value1".getBytes(), metadata.getValue("group1", "key1"));
    }

    @Test
    public void testGetValue_withEmptyByteArray() {
        Map<String, Map<String, byte[]>> data = new HashMap<>();
        Map<String, byte[]> group1 = new HashMap<>();
        group1.put("emptyKey", new byte[0]);
        data.put("group1", group1);

        SystemMetadata metadata = new SystemMetadata(data);
        byte[] value = metadata.getValue("group1", "emptyKey");

        assertNotNull(value);
        assertEquals(0, value.length);
    }
}
