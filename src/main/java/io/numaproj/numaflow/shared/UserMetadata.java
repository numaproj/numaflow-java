package io.numaproj.numaflow.shared;

import common.MetadataOuterClass;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * UserMetadata is mapping of group name to key-value pairs
 * UserMetadata wraps user generated metadata groups per message.
 * It can be appended to and passed on to the downstream.
 * Note: All the null checks have been added within the constructors to ensure
 * that the data and its entries are always valid.
 */
public class UserMetadata {
    private final Map<String, Map<String, byte[]>> data;

    /**
     * Default constructor for UserMetadata.
     * Initializes an empty HashMap data.
     */
    public UserMetadata() {
        this.data = new HashMap<>();
    }

    /**
     * Constructor from MetadataOuterClass.Metadata.
     * Initializes an empty HashMap data if the metadata passed is null or empty.
     * For each entry in {@code metadata.getUserMetadataMap()}, it creates a new HashMap and copies the key-value pairs
     * from the entry to the new HashMap. It also filters out any null values.
     *
     * @param metadata is an instance of MetadataOuterClass.Metadata which contains user metadata
     */
    public UserMetadata(MetadataOuterClass.Metadata metadata) {
        if (metadata == null || metadata.getUserMetadataMap().isEmpty()) {
            this.data = new HashMap<>();
            return;
        }
        // Copy the data to prevent mutation
        this.data = metadata.getUserMetadataMap()
                .entrySet()
                .stream()
                // No null checks here as protobuf contract ensures that the data has no null values
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue()
                                .getKeyValueMap().entrySet().stream()
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e1 -> e1.getValue().toByteArray()
                                ))
                        )
                );
    }

    /**
     * Copy constructor for UserMetadata.
     * Returns a UserMetadata with empty HashMap data if the userMetadata passed is null or
     * {@code userMetadata.data} is null.
     * For each entry in {@code userMetadata.data}, it creates a new HashMap and copies the key-value pairs
     * from the entry to the new HashMap. It also filters out any null values.
     *
     * @param userMetadata the user metadata to copy
     */
    public UserMetadata(UserMetadata userMetadata) {
        if (userMetadata == null || userMetadata.data == null) {
            this.data = new HashMap<>();
            return;
        }

        // Deep copy the data to prevent mutation
        this.data = userMetadata.data.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().entrySet()
                                .stream()
                                .filter(e1 -> e1.getValue() != null)
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e1 -> e1.getValue().clone()
                                ))
                ));
    }

    /**
     * Convert the user metadata to a map that can be used to create MetadataOuterClass.Metadata
     * For each entry in {@code data}, it creates a new MetadataOuterClass.KeyValueGroup and copies the key-value pairs
     * from the entry to the new MetadataOuterClass.KeyValueGroup.
     * It also filters out any null values.
     *
     * @return MetadataOuterClass.Metadata
     */
    public MetadataOuterClass.Metadata toProto() {
        Map<String, MetadataOuterClass.KeyValueGroup> result = new HashMap<>();
        this.data.forEach((group, kvMap) -> {
            MetadataOuterClass.KeyValueGroup.Builder builder = MetadataOuterClass.KeyValueGroup.newBuilder();
            // No null checks required as the constructor and add methods ensures that the data is valid
            kvMap.forEach((key, value) -> builder.putKeyValue(key, ByteString.copyFrom(value)));

            result.put(group, builder.build());
        });

        return MetadataOuterClass.Metadata
                .newBuilder()
                .putAllUserMetadata(result)
                .build();
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
     * Delete a group from the user metadata
     *
     * @param group is the name of the group to delete
     */
    public void deleteGroup(String group) {
        this.data.remove(group);
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
     * Delete a key from a group
     *
     * @param group Name of the group containing the key
     * @param key Name of the key to delete
     */
    public void deleteKey(String group, String key) {
        if (!this.data.containsKey(group)) {
            return;
        }
        this.data.get(group).remove(key);
    }

    /**
     * Add a key value pair to a group
     * Note: If the value is null, the key/value pair will not be added to the group
     *
     * @param group Name of the group to which key value pairs are to be added
     * @param key Name of the key in group to which the value is to be added
     * @param value Value to be added to the key
     */
    public void addKV(String group, String key, byte[] value) {
        // null values are not added
        if (group != null && key != null && value != null) {
            this.data
                    .computeIfAbsent(group, k -> new HashMap<>())
                    .put(key, value.clone());
        }
    }

    /**
     * Add multiple key value pairs to a group
     * Note: If the value is null, the key/value pair will not be added to the group
     *
     * @param group Name of the group to which key value pairs are to be added
     * @param kv Map of key value pairs to be added to the group
     */
    public void addKVs(String group, Map<String, byte[]> kv) {
        if (group == null || kv == null) {
            return;
        }

        this.data.computeIfAbsent(
                group,
                k -> kv.entrySet().stream()
                        .filter(e -> e.getKey() != null && e.getValue() != null)
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> e.getValue().clone()
                                )
                        )
        );
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
        // null value should not be present but check added for safety
        return value == null ? null : value.clone();
    }

    /**
     * Clear all the user metadata
     */
    public void clear() {
        this.data.clear();
    }
}
