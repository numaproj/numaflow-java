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

        assertEquals(2, metadata.getGroups().size());
        assertArrayEquals("value1".getBytes(), metadata.getValue("group1" ,"key1"));
        assertArrayEquals("valueA".getBytes(), metadata.getValue("group2", "keyA"));
    }

    @Test
    public void testProtoConstructor_withNullMetadata() {
        SystemMetadata metadata = new SystemMetadata((MetadataOuterClass.Metadata) null);
        assertNotNull(metadata.getGroups());
        assertTrue(metadata.getGroups().isEmpty());
    }

    @Test
    public void testProtoConstructor_withEmptyMetadata() {
        MetadataOuterClass.Metadata protoMetadata = MetadataOuterClass.Metadata.newBuilder().build();
        SystemMetadata metadata = new SystemMetadata(protoMetadata);
        assertNotNull(metadata.getGroups());
        assertTrue(metadata.getGroups().isEmpty());
    }

    @Test
    public void testGetKeys_withMissingGroup() {
        SystemMetadata metadata = new SystemMetadata();
        assertNotNull(metadata.getKeys("missing"));
        assertTrue(metadata.getKeys("missing").isEmpty());
    }

    @Test
    public void testGetKeys_withExistingGroup() {
        MetadataOuterClass.KeyValueGroup kvGroup = MetadataOuterClass.KeyValueGroup.newBuilder()
                .putKeyValue("key1", ByteString.copyFromUtf8("value1"))
                .putKeyValue("key2", ByteString.copyFromUtf8("value2"))
                .build();

        MetadataOuterClass.Metadata protoMetadata = MetadataOuterClass.Metadata.newBuilder()
                .putSysMetadata("group1", kvGroup)
                .build();

        SystemMetadata metadata = new SystemMetadata(protoMetadata);

        List<String> keys = metadata.getKeys("group1");
        assertEquals(2, keys.size());
        assertTrue(keys.contains("key1"));
        assertTrue(keys.contains("key2"));
    }

    @Test
    public void testGetValue_withMissingKey() {
        MetadataOuterClass.KeyValueGroup kvGroup = MetadataOuterClass.KeyValueGroup.newBuilder()
                .putKeyValue("key1", ByteString.copyFromUtf8("value1"))
                .build();

        MetadataOuterClass.Metadata protoMetadata = MetadataOuterClass.Metadata.newBuilder()
                .putSysMetadata("group1", kvGroup)
                .build();

        SystemMetadata metadata = new SystemMetadata(protoMetadata);

        assertNull(metadata.getValue("group1", "missingKey"));
    }

    @Test
    public void testGetValue_returnsClone() {
        MetadataOuterClass.KeyValueGroup kvGroup = MetadataOuterClass.KeyValueGroup.newBuilder()
                .putKeyValue("key1", ByteString.copyFromUtf8("value1"))
                .build();

        MetadataOuterClass.Metadata protoMetadata = MetadataOuterClass.Metadata.newBuilder()
                .putSysMetadata("group1", kvGroup)
                .build();

        SystemMetadata metadata = new SystemMetadata(protoMetadata);

        byte[] got = metadata.getValue("group1", "key1");
        assertArrayEquals("value1".getBytes(), got);

        got[0] = 'X';

        assertArrayEquals("value1".getBytes(), metadata.getValue("group1", "key1"));
    }

    @Test
    public void testGetGroups_returnsCopy_notLiveView() {
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

    @Test
    public void testGetKeys_returnsCopy_notLiveView() {
        MetadataOuterClass.KeyValueGroup kvGroup = MetadataOuterClass.KeyValueGroup.newBuilder()
                .putKeyValue("key1", ByteString.copyFromUtf8("value1"))
                .build();

        MetadataOuterClass.Metadata protoMetadata = MetadataOuterClass.Metadata.newBuilder()
                .putSysMetadata("group1", kvGroup)
                .build();

        SystemMetadata metadata = new SystemMetadata(protoMetadata);

        List<String> keys = metadata.getKeys("group1");
        assertEquals(1, keys.size());

        keys.clear();

        assertEquals(1, metadata.getKeys("group1").size());
        assertTrue(metadata.getKeys("group1").contains("key1"));
    }
}
