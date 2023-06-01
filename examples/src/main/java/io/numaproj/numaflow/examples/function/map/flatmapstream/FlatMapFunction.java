package io.numaproj.numaflow.examples.function.map.flatmapstream;

import io.numaproj.numaflow.function.FunctionServer;
import io.numaproj.numaflow.function.handlers.MapStreamHandler;
import io.numaproj.numaflow.function.interfaces.Datum;
import io.numaproj.numaflow.function.types.Message;

/**
 * This is a simple User Defined Function example which processes the input message
 * and produces more than one output messages(flatMap) in a streaming mode
 * example : if the input message is "dog,cat", it streams two output messages
 * "dog" and "cat"
 */

public class FlatMapStreamFunction extends MapStreamHandler {

    public static void main(String[] args) throws Exception {
        new FunctionServer().registerMapStreamHandler(new FlatMapStreamFunction()).start();
    }

    public void processMessage(String[] keys, Datum datum, StreamObserver<Udfunction.DatumResponse> streamObserver) {
        String msg = new String(data.getValue());
        String[] strs = msg.split(",");

        for (String str : strs) {
            onNext(new Message(str.getBytes()), streamObserver);
        }
    }
}
