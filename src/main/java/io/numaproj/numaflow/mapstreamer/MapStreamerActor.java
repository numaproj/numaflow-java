package io.numaproj.numaflow.mapstreamer;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import io.numaproj.numaflow.map.v1.MapOuterClass;

import java.time.Instant;

/**
 * MapStreamerActor processes individual requests.
 * Upon processing, it sends the results or errors back to the MapStreamSupervisorActor.
 * It stops itself after processing the request.
 */
class MapStreamerActor extends AbstractActor {

    private final MapStreamer mapStreamer;

    public MapStreamerActor(MapStreamer mapStreamer) {
        this.mapStreamer = mapStreamer;
    }

    public static Props props(MapStreamer mapStreamer) {
        return Props.create(MapStreamerActor.class, mapStreamer);
    }

    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(MapOuterClass.MapRequest.class, this::processRequest)
                .build();
    }

    private void processRequest(MapOuterClass.MapRequest mapRequest) {
        HandlerDatum handlerDatum = new HandlerDatum(
                mapRequest.getRequest().getValue().toByteArray(),
                Instant.ofEpochSecond(
                        mapRequest.getRequest().getWatermark().getSeconds(),
                        mapRequest.getRequest().getWatermark().getNanos()),
                Instant.ofEpochSecond(
                        mapRequest.getRequest().getEventTime().getSeconds(),
                        mapRequest.getRequest().getEventTime().getNanos()),
                mapRequest.getRequest().getHeadersMap()
        );

        String[] keys = mapRequest.getRequest().getKeysList().toArray(new String[0]);

        try {
            mapStreamer.processMessage(keys, handlerDatum, new OutputObserverImpl(getSender()));
        } catch (Exception e) {
            getSender().tell(e, getSelf());
        }
        context().stop(getSelf());
    }
}
