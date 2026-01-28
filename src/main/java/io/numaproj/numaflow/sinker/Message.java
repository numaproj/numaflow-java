package io.numaproj.numaflow.sinker;

import com.google.protobuf.ByteString;
import common.MetadataOuterClass;
import io.numaproj.numaflow.shared.UserMetadata;
import io.numaproj.numaflow.sink.v1.SinkOuterClass;
import lombok.Builder;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Message contains information that needs to be sent to the OnSuccess sink.
 * The message can be different from the original message that was sent to primary sink.
 */
@Getter
@Builder
public class Message {
    private final byte[] value;
    private final String[] keys;
    /**
     * userMetadata is the user defined metadata that is added to the onSuccess message
     * This is using the common {@link UserMetadata} class to allow reusing the user metadata stored in Datum
     */
    private final UserMetadata userMetadata;

    /**
     * Static method to create an onSuccess message from a sinker Datum object.
     *
     * @param datum object used to create the onSuccess message.
     * The created onSuccess message will have the same value, keys and userMetadata as the original datum
     * @return onSuccess message
     */
    public static Message fromDatum(Datum datum) {
        if (datum == null) {
            return Message.builder().build();
        }
        return Message.builder()
                .value(datum.getValue())
                .keys(datum.getKeys())
                .userMetadata(datum.getUserMetadata())
                .build();
    }

    /**
     * Static method to convert a Message object to a SinkOuterClass.SinkResponse.Result.Message object.
     * If the message is null, returns the default instance of SinkOuterClass.SinkResponse.Result.Message.
     *
     * @param message The message object to convert into the relevant proto object
     * @return The converted proto object
     */
    public static SinkOuterClass.SinkResponse.Result.Message toProto(Message message) {
        if (message == null) {
            return SinkOuterClass.SinkResponse.Result.Message.getDefaultInstance();
        }
        return SinkOuterClass.SinkResponse.Result.Message.newBuilder()
                .addAllKeys(message.getKeys()
                        == null ? new ArrayList<>() : Arrays.asList(message.getKeys()))
                .setValue(message.getValue()
                        == null ? ByteString.EMPTY : ByteString.copyFrom(message.getValue()))
                .setMetadata(message.getUserMetadata()
                        == null ? MetadataOuterClass.Metadata.getDefaultInstance()
                        : message.getUserMetadata().toProto())
                .build();
    }
}


