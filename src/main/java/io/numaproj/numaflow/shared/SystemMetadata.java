package io.numaproj.numaflow.shared;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import common.MetadataOuterClass;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * SystemMetadata is mapping of group name to key-value pairs
 * SystemMetadata wraps system-generated metadata groups per message.
 * It is read-only to UDFs
 */
@Getter
@AllArgsConstructor
public class SystemMetadata {
    private final Map<String, Map<String, byte[]>> data;

    /**
     * Default constructor
     */
    public SystemMetadata() {
        this.data = new HashMap<>();
    }

    /**
     * Constructor from MetadataOuterClass.Metadata
     *
     * @param metadata is an instance of MetadataOuterClass.Metadata which contains system metadata
     */
    public SystemMetadata(MetadataOuterClass.Metadata metadata) {
        if (metadata == null || metadata.getSysMetadataMap().isEmpty()) {
            this.data = new HashMap<>();
            return;
        }
        this.data = metadata.getSysMetadataMap().entrySet().stream()
            .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> new HashMap<>(e.getValue()
                            .getKeyValueMap()
                            .entrySet()
                            .stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    e1 -> e1.getValue().toByteArray()
                            ))
                    )
            ));
    }

    /**
     * Get the list of all groups present in the user metadata
     *
     * @return list of group names
     */
    public List<String> getGroups() {
        if (this.data == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(this.data.keySet());
    }

    /**
     * Get a list of key names within a given group
     *
     * @param group is the name of the group from which to get the key names
     * @return a list of key names within the group
     */
    public List<String> getKeys(String group) {
        if (this.data == null || !this.data.containsKey(group)) {
            return new ArrayList<>();
        }
        return new ArrayList<>(this.data.get(group).keySet());
    }

    /**
     * Get the value of a key in a group
     *
     * @param group Name of the group which contains the key holding required value
     * @param key Name of the key in the group for which value is required
     * @return Value of the key in the group or null if the group/key is not present
     */
    public byte[] getValue(String group, String key) {
        if (this.data == null) {
            return null;
        }
        Map<String, byte[]> groupData = this.data.get(group);
        if (groupData == null) {
            return null;
        }
        byte[] value = groupData.get(key);
        return value == null ? null : value.clone();
    }
}
