package io.numaproj.numaflow.shared;

import common.MetadataOuterClass;
import lombok.AllArgsConstructor;
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
 * {@link lombok.AllArgsConstructor} allows creation of UserMetadata from a `Map<String, Map<String, byte[]>>`
 * Copy constructor allows creation of UserMetadata from another UserMetadata
 */
@Getter
@AllArgsConstructor
public class UserMetadata {
    private final Map<String, Map<String, byte[]>> data;

    /**
     * Default constructor
     */
    public UserMetadata() {
        this.data = new HashMap<>();
    }

    /**
     * Constructor from MetadataOuterClass.Metadata
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
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new HashMap<>(e.getValue().getKeyValueMap().entrySet().stream()
                                .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e1 -> e1.getValue().toByteArray()
                                ))
                        )
                ));
    }

    /**
     * Copy constructor
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
     *
     * @return MetadataOuterClass.Metadata
     */
    public MetadataOuterClass.Metadata toProto() {
        if (this.data == null) {
            return MetadataOuterClass.Metadata.getDefaultInstance();
        }

        Map<String, MetadataOuterClass.KeyValueGroup> result = new HashMap<>();
        this.data.forEach((group, kvMap) -> {
            MetadataOuterClass.KeyValueGroup.Builder builder = MetadataOuterClass.KeyValueGroup.newBuilder();
            if (kvMap != null) {
                kvMap.forEach((key, value) -> {
                    if (value == null) {
                        builder.putKeyValue(key, ByteString.EMPTY);
                    } else {
                        builder.putKeyValue(key, ByteString.copyFrom(value));
                    }
                });
            }
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
        if (this.data == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(this.data.keySet());
    }

    /**
     * Delete a group from the user metadata
     *
     * @param group is the name of the group to delete
     */
    public void deleteGroup(String group) {
        if (this.data == null) {
            return;
        }
        this.data.remove(group);
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
     * Delete a key from a group
     *
     * @param group Name of the group containing the key
     * @param key Name of the key to delete
     */
    public void deleteKey(String group, String key) {
        if (this.data == null || !this.data.containsKey(group)) {
            return;
        }
        this.data.get(group).remove(key);
    }

    /**
     * Add a key value pair to a group
     *
     * @param group Name of the group to which key value pairs are to be added
     * @param key Name of the key in group to which the value is to be added
     * @param value Value to be added to the key
     */
    public void addKV(String group, String key, byte[] value) {
        if (this.data == null) {
            return;
        }
        this.data.computeIfAbsent(group, k -> new HashMap<>()).put(key, value.clone());
    }

    /**
     * Add multiple key value pairs to a group
     *
     * @param group Name of the group to which key value pairs are to be added
     * @param kv Map of key value pairs to be added to the group
     */
    public void addKVs(String group, Map<String, byte[]> kv) {
        if (this.data == null) {
            return;
        }
        Map<String, byte[]> groupMap = this.data.computeIfAbsent(group, k -> new HashMap<>());
        // Copy the values to prevent mutation
        kv.forEach((key, value) -> groupMap.put(key, value.clone()));
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

    /**
     * Clear all the user metadata
     */
    public void clear() {
        if (this.data == null) {
            return;
        }
        this.data.clear();
    }
}
