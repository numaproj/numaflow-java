package io.numaproj.numaflow.examples.mapstream.flatmapstream;

import io.numaproj.numaflow.mapstreamer.*;


/**
 * This is a simple User Defined Function example which processes the input message
 * and produces more than one output messages(flatMap) in a streaming mode
 * example : if the input message is "dog,cat", it streams two output messages
 * "dog" and "cat"
 */

public class FlatMapStreamFunction extends MapStreamer {

    public static void main(String[] args) throws Exception {
        Server server = new Server(new FlatMapStreamFunction());

        // Start the server
        server.start();

        // wait for the server to shutdown
        server.awaitTermination();
    }

    public void processMessage(String[] keys, Datum data, OutputObserver outputObserver) {
        String msg = new String(data.getValue());
        String[] strs = msg.split(",");

        for (String str : strs) {
            outputObserver.send(new Message(str.getBytes()));
        }
    }
}
