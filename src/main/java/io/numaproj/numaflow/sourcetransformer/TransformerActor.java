package io.numaproj.numaflow.sourcetransformer;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import common.MetadataOuterClass;
import io.numaproj.numaflow.shared.SystemMetadata;
import io.numaproj.numaflow.shared.UserMetadata;
import io.numaproj.numaflow.sourcetransformer.v1.Sourcetransformer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * TransformerActor is an actor that processes the SourceTransformRequest.
 * It invokes the transformer to process the request and sends the response back to the sender actor (TransformSupervisorActor).
 * In case of any exception, it sends the exception back to the sender actor.
 * It stops itself after processing the request.
 */
class TransformerActor extends AbstractActor {
    private final SourceTransformer transformer;

    /**
     * Constructor for TransformerActor.
     *
     * @param transformer The transformer to be used for processing the request.
     */
    public TransformerActor(SourceTransformer transformer) {
        this.transformer = transformer;
    }

    /**
     * Creates Props for a TransformerActor.
     *
     * @param transformer The transformer to be used for processing the request.
     *
     * @return a Props for creating a TransformerActor.
     */
    public static Props props(SourceTransformer transformer) {
        return Props.create(TransformerActor.class, transformer);
    }

    /**
     * Defines the initial actor behavior, i.e., what it does on startup and when it begins to process messages.
     *
     * @return a Receive object defining the initial behavior of the actor.
     */
    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(Sourcetransformer.SourceTransformRequest.class, this::processRequest)
                .build();
    }

    /**
     * Processes the SourceTransformRequest and sends the response back to the sender actor.
     *
     * @param transformRequest The SourceTransformRequest to be processed.
     */
    private void processRequest(Sourcetransformer.SourceTransformRequest transformRequest) {
        Sourcetransformer.SourceTransformRequest.Request request = transformRequest.getRequest();
        HandlerDatum handlerDatum = new HandlerDatum(
                request.getValue().toByteArray(),
                Instant.ofEpochSecond(
                        request.getWatermark().getSeconds(),
                        request.getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        request.getEventTime().getSeconds(),
                        request.getEventTime().getNanos()),
                request.getHeadersMap(),
                new UserMetadata(request.getMetadata()),
                new SystemMetadata(request.getMetadata())
        );
        String[] keys = request.getKeysList().toArray(new String[0]);
        try {
            MessageList resultMessages = this.transformer.processMessage(keys, handlerDatum);
            Sourcetransformer.SourceTransformResponse response = buildResponse(
                    resultMessages,
                    request.getId());
            getSender().tell(response, getSelf());
        } catch (Exception e) {
            getSender().tell(e, getSelf());
        }
        context().stop(getSelf());
    }

    /**
     * Builds the SourceTransformResponse from the MessageList.
     *
     * @param messageList The MessageList to be transformed into a SourceTransformResponse.
     *
     * @return The built SourceTransformResponse.
     */

    private Sourcetransformer.SourceTransformResponse buildResponse(
            MessageList messageList,
            String ID) {
        // Users should not send null as the response, we will let client handle it.
        if (messageList == null) {
            return Sourcetransformer.SourceTransformResponse
                    .newBuilder()
                    .setId(ID)
                    .build();
        }
        Sourcetransformer.SourceTransformResponse.Builder responseBuilder = Sourcetransformer
                .SourceTransformResponse
                .newBuilder();

        messageList.getMessages().forEach(message -> {
            responseBuilder.addResults(Sourcetransformer.SourceTransformResponse.Result.newBuilder()
                    .setValue(message.getValue() == null ? ByteString.EMPTY : ByteString.copyFrom(
                            message.getValue()))
                    .setEventTime(Timestamp.newBuilder()
                            .setSeconds(message
                                    .getEventTime()
                                    .getEpochSecond())
                            .setNanos(message.getEventTime().getNano()))
                    .addAllKeys(message.getKeys()
                            == null ? new ArrayList<>() : Arrays.asList(message.getKeys()))
                    .addAllTags(message.getTags()
                            == null ? new ArrayList<>() : Arrays.asList(message.getTags()))
                    .setMetadata(message.getUserMetadata()
                            == null ? MetadataOuterClass.Metadata.getDefaultInstance()
                            : message.getUserMetadata().toProto())
                    .build());
        });
        return responseBuilder.setId(ID).build();
    }
}
