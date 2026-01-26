package io.numaproj.numaflow.shared;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import common.MetadataOuterClass;
import lombok.Getter;

/**
 * SystemMetadata is mapping of group name to key-value pairs
 * SystemMetadata wraps system-generated metadata groups per message.
 * It is read-only to UDFs
 */
@Getter
public class SystemMetadata {
    private final Map<String, Map<String, byte[]>> data;

    /**
     * Default constructor
     */
    public SystemMetadata() {
        this.data = new HashMap<>();
    }

    /**
     * An all args constructor that filters out null values and copies the values to prevent mutation.
     * If the data is null or empty, it initializes an empty HashMap data.
     * For each entry in {@code data}, it creates a new HashMap and copies the key-value pairs
     * from the entry to the new HashMap. It also filters out any null values.
     * Empty hashmap values are allowed as entries in the data map.
     *
     * @param data is a map of group name to key-value pairs
     */
    public SystemMetadata(Map<String, Map<String, byte[]>> data) {
        if (data == null || data.isEmpty()) {
            this.data = new HashMap<>();
            return;
        }
        this.data = data.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> entry.getValue().entrySet().stream()
                                .filter(e1 -> e1.getValue() != null)
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e1-> e1.getValue().clone()
                                ))
                ));
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
     * Get the data as a map.
     * Returns a deep copy of the data to prevent mutation.
     *
     * @return a deep copy of the data
     */
    public Map<String, Map<String, byte[]>> getData() {
        // Deep copy the data to prevent mutation
        return this.data.entrySet().stream()
                // No null checks required as the constructor ensures that the data is valid
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> entry.getValue().entrySet().stream()
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e1-> e1.getValue().clone()
                                ))
                ));
    }

    /**
     * Get the list of all groups present in the user metadata
     *
     * @return list of group names
     */
    public List<String> getGroups() {
        return new ArrayList<>(this.data.keySet());
    }

    /**
     * Get a list of key names within a given group
     *
     * @param group is the name of the group from which to get the key names
     * @return a list of key names within the group
     */
    public List<String> getKeys(String group) {
        if (!this.data.containsKey(group)) {
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
        Map<String, byte[]> groupData = this.data.get(group);
        if (groupData == null) {
            return null;
        }
        byte[] value = groupData.get(key);
        return value == null ? null : value.clone();
    }
}
