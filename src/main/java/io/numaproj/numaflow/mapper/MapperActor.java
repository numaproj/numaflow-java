package io.numaproj.numaflow.mapper;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.google.protobuf.ByteString;
import common.MetadataOuterClass;
import io.numaproj.numaflow.map.v1.MapOuterClass;
import io.numaproj.numaflow.shared.SystemMetadata;
import io.numaproj.numaflow.shared.UserMetadata;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Mapper actor that processes the map request. It invokes the mapper to process the request and
 * sends the response back to the sender actor(MapSupervisorActor). In case of any exception, it
 * sends the exception back to the sender actor. It stops itself after processing the request.
 */
class MapperActor extends AbstractActor {
    private final Mapper mapper;

    public MapperActor(Mapper mapper) {
        this.mapper = mapper;
    }

    public static Props props(Mapper mapper) {
        return Props.create(MapperActor.class, mapper);
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(MapOuterClass.MapRequest.class, this::processRequest)
                .build();
    }

    /**
     * Process the map request and send the response back to the sender actor.
     *
     * @param mapRequest map request
     */
    private void processRequest(MapOuterClass.MapRequest mapRequest) {
        Datum handlerDatum = new HandlerDatum(
                mapRequest.getRequest().getValue().toByteArray(),
                Instant.ofEpochSecond(
                        mapRequest.getRequest().getWatermark().getSeconds(),
                        mapRequest.getRequest().getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        mapRequest.getRequest().getEventTime().getSeconds(),
                        mapRequest.getRequest().getEventTime().getNanos()),
                mapRequest.getRequest().getHeadersMap(),
                new UserMetadata(mapRequest.getRequest().getMetadata()),
                new SystemMetadata(mapRequest.getRequest().getMetadata())
        );
        String[] keys = mapRequest.getRequest().getKeysList().toArray(new String[0]);
        try {
            MessageList resultMessages = this.mapper.processMessage(keys, handlerDatum);
            MapOuterClass.MapResponse response = buildResponse(resultMessages, mapRequest.getId());
            getSender().tell(response, getSelf());
        } catch (Exception e) {
            getSender().tell(e, getSelf());
        }
        context().stop(getSelf());
    }

    /**
     * Build the response from the message list.
     *
     * @param messageList message list
     *
     * @return map response
     */
    private MapOuterClass.MapResponse buildResponse(MessageList messageList, String ID) {
        // Users should not send null as the response, we will let client handle it.
        if (messageList == null) {
            return MapOuterClass.MapResponse
                    .newBuilder()
                    .setId(ID)
                    .build();
        }

        MapOuterClass.MapResponse.Builder responseBuilder = MapOuterClass
                .MapResponse
                .newBuilder();

        messageList.getMessages().forEach(message -> {
            responseBuilder.addResults(MapOuterClass.MapResponse.Result.newBuilder()
                    .setValue(message.getValue() == null ? ByteString.EMPTY : ByteString.copyFrom(
                            message.getValue()))
                    .addAllKeys(message.getKeys()
                            == null ? new ArrayList<>() : Arrays.asList(message.getKeys()))
                    .addAllTags(message.getTags()
                            == null ? new ArrayList<>() : Arrays.asList(message.getTags()))
                    .setMetadata(message.getUserMetadata()
                            == null ? MetadataOuterClass.Metadata.getDefaultInstance() : message.getUserMetadata().toProto())
                    .build());
        });
        return responseBuilder.setId(ID).build();
    }
}
