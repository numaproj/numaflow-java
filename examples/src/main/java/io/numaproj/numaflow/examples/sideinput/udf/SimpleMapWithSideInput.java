package io.numaproj.numaflow.examples.sideinput.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.numaproj.numaflow.examples.sideinput.simple.Config;
import io.numaproj.numaflow.mapper.Datum;
import io.numaproj.numaflow.mapper.Mapper;
import io.numaproj.numaflow.mapper.Message;
import io.numaproj.numaflow.mapper.MessageList;
import io.numaproj.numaflow.mapper.Server;
import io.numaproj.numaflow.sideinput.SideInputRetriever;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a simple User Defined Map example with side input support.
 */

@Slf4j
public class SimpleMapWithSideInput extends Mapper {
    SideInputWatcher sideInputWatcher;
    ObjectMapper objectMapper = new ObjectMapper();
    Config config = new Config("sampling", 0.5F);

    public SimpleMapWithSideInput(SideInputWatcher sideInputWatcher) {
        this.sideInputWatcher = sideInputWatcher;
    }

    public static void main(String[] args) throws Exception {
        String sideInputName = "sampling-input";
        // Get the side input path and file from the environment variables
        String dirPath = SideInputRetriever.SIDE_INPUT_DIR;

        // Watch for side input
        SideInputWatcher sideInputWatcher = new SideInputWatcher(dirPath, sideInputName);
        sideInputWatcher.startWatching();

        // start the server
        new Server(new SimpleMapWithSideInput(sideInputWatcher)).start();

        // Stop watching for side input
        sideInputWatcher.stopWatching();
    }

    public MessageList processMessage(String[] keys, Datum data) {
        // Get the side input
        String sideInput = sideInputWatcher.getSideInput();
        try {
            config = objectMapper.readValue(sideInput, Config.class);
        } catch (JsonProcessingException e) {
            return MessageList.newBuilder().addMessage(Message.toDrop()).build();
        }

        log.info("side input - {}", config.toString());
        return MessageList
                .newBuilder()
                .addMessage(new Message(data.getValue(), keys))
                .build();
    }
}
