package io.numaproj.numaflow.examples.sideinput.udf;

import io.numaproj.numaflow.mapper.Datum;
import io.numaproj.numaflow.mapper.Mapper;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import io.numaproj.numaflow.mapper.Server;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a simple User Defined Map example with side input support.
 */

@Slf4j
@AllArgsConstructor
public class SimpleMapWithSideInput extends Mapper {
    SideInputWatcher sideInputWatcher;

    public static void main(String[] args) throws Exception {
        // Get the side input path and file from the environment variables
        String dirPath = System.getenv("SIDEINPUT_PATH");
        String filePath = System.getenv("SIDEINPUT_FILE");

        // Watch for side input
        SideInputWatcher sideInputWatcher = new SideInputWatcher(dirPath, filePath);
        sideInputWatcher.startWatching();

        // start the server
        new Server(new SimpleMapWithSideInput(sideInputWatcher)).start();

        // Stop watching for side input
        sideInputWatcher.stopWatching();
    }

    public MessageList processMessage(String[] keys, Datum data) {
        // Get the side input
        String sideInput = sideInputWatcher.getSideInput();
        log.info("side input - {}", sideInput);
        return MessageList
                .newBuilder()
                .addMessage(new Message(data.getValue(), keys))
                .build();
    }
}
