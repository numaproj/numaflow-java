package io.numaproj.numaflow.examples.function.map.flatmapstream;

import io.grpc.stub.StreamObserver;
import io.numaproj.numaflow.mapstream.v1.Mapstream;
import io.numaproj.numaflow.mapstreamer.Datum;
import io.numaproj.numaflow.mapstreamer.MapStreamer;
import io.numaproj.numaflow.mapstreamer.Message;
import io.numaproj.numaflow.mapstreamer.Server;


/**
 * This is a simple User Defined Function example which processes the input message
 * and produces more than one output messages(flatMap) in a streaming mode
 * example : if the input message is "dog,cat", it streams two output messages
 * "dog" and "cat"
 */

public class FlatMapStreamFunction extends MapStreamer {

    public static void main(String[] args) throws Exception {
        new Server(new FlatMapStreamFunction()).start();
    }

    public void processMessage(String[] keys, Datum data, StreamObserver<Mapstream.MapStreamResponse.Result> streamObserver) {
        String msg = new String(data.getValue());
        String[] strs = msg.split(",");

        for (String str : strs) {
            onNext(new Message(str.getBytes()), streamObserver);
        }
    }
}
