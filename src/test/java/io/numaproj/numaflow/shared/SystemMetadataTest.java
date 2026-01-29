package io.numaproj.numaflow.shared;

import com.google.protobuf.ByteString;
import common.MetadataOuterClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class SystemMetadataTest {

    @Test
    public void testDefaultConstructor() {
        SystemMetadata metadata = new SystemMetadata();
        assertNotNull(metadata.getGroups());
        assertTrue(metadata.getGroups().isEmpty());
    }

    @Test
    public void testProtoConstructor() {
        // test with valid metadata
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

        SystemMetadata metadata1 = new SystemMetadata(protoMetadata);

        assertEquals(2, metadata1.getGroups().size());
        assertArrayEquals("value1".getBytes(), metadata1.getValue("group1" ,"key1"));
        assertArrayEquals("valueA".getBytes(), metadata1.getValue("group2", "keyA"));

        // test with null metadata
        SystemMetadata metadata2 = new SystemMetadata(null);
        assertNotNull(metadata2.getGroups());
        assertTrue(metadata2.getGroups().isEmpty());

        // test with empty metadata
        MetadataOuterClass.Metadata protoMetadata2 = MetadataOuterClass.Metadata.newBuilder().build();
        SystemMetadata metadata3 = new SystemMetadata(protoMetadata2);
        assertNotNull(metadata3.getGroups());
        assertTrue(metadata3.getGroups().isEmpty());
    }

    @Test
    public void testGetKeys() {
        // test with missing group
        SystemMetadata metadata1 = new SystemMetadata();
        assertNotNull(metadata1.getKeys("missing"));
        assertTrue(metadata1.getKeys("missing").isEmpty());

        // test with existing group
        MetadataOuterClass.KeyValueGroup kvGroup1 = MetadataOuterClass.KeyValueGroup.newBuilder()
                .putKeyValue("key1", ByteString.copyFromUtf8("value1"))
                .putKeyValue("key2", ByteString.copyFromUtf8("value2"))
                .build();

        MetadataOuterClass.Metadata protoMetadata1 = MetadataOuterClass.Metadata.newBuilder()
                .putSysMetadata("group1", kvGroup1)
                .build();

        SystemMetadata metadata2 = new SystemMetadata(protoMetadata1);

        List<String> keys1 = metadata2.getKeys("group1");
        assertEquals(2, keys1.size());
        assertTrue(keys1.contains("key1"));
        assertTrue(keys1.contains("key2"));

        // test if getKeys returns a copy
        MetadataOuterClass.KeyValueGroup kvGroup2 = MetadataOuterClass.KeyValueGroup.newBuilder()
                .putKeyValue("key1", ByteString.copyFromUtf8("value1"))
                .build();

        MetadataOuterClass.Metadata protoMetadata2 = MetadataOuterClass.Metadata.newBuilder()
                .putSysMetadata("group1", kvGroup2)
                .build();

        SystemMetadata metadata3 = new SystemMetadata(protoMetadata2);

        List<String> keys2 = metadata3.getKeys("group1");
        assertEquals(1, keys2.size());

        keys2.clear();

        assertEquals(1, metadata3.getKeys("group1").size());
        assertTrue(metadata3.getKeys("group1").contains("key1"));
    }
    @Test
    public void testGetValue() {
        // test with missing key
        MetadataOuterClass.KeyValueGroup kvGroup2 = MetadataOuterClass.KeyValueGroup.newBuilder()
                .putKeyValue("key1", ByteString.copyFromUtf8("value1"))
                .build();

        MetadataOuterClass.Metadata protoMetadata1 = MetadataOuterClass.Metadata.newBuilder()
                .putSysMetadata("group1", kvGroup2)
                .build();

        SystemMetadata metadata1 = new SystemMetadata(protoMetadata1);

        assertNull(metadata1.getValue("group1", "missingKey"));

        // test if value is cloned
        MetadataOuterClass.KeyValueGroup kvGroup = MetadataOuterClass.KeyValueGroup.newBuilder()
                .putKeyValue("key1", ByteString.copyFromUtf8("value1"))
                .build();

        MetadataOuterClass.Metadata protoMetadata = MetadataOuterClass.Metadata.newBuilder()
                .putSysMetadata("group1", kvGroup)
                .build();

        SystemMetadata metadata2 = new SystemMetadata(protoMetadata);

        byte[] got = metadata2.getValue("group1", "key1");
        assertArrayEquals("value1".getBytes(), got);

        got[0] = 'X';

        assertArrayEquals("value1".getBytes(), metadata2.getValue("group1", "key1"));
    }

    @Test
    public void testGetGroups() {
        MetadataOuterClass.KeyValueGroup kvGroup = MetadataOuterClass.KeyValueGroup.newBuilder()
                .putKeyValue("key1", ByteString.copyFromUtf8("value1"))
                .build();

        MetadataOuterClass.Metadata protoMetadata = MetadataOuterClass.Metadata.newBuilder()
                .putSysMetadata("group1", kvGroup)
                .build();

        SystemMetadata metadata = new SystemMetadata(protoMetadata);

        List<String> groups = metadata.getGroups();
        assertEquals(1, groups.size());

        groups.clear();

        assertEquals(1, metadata.getGroups().size());
        assertTrue(metadata.getGroups().contains("group1"));
    }

}
