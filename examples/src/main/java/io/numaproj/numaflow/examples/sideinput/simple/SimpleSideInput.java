package io.numaproj.numaflow.examples.sideinput.simple;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.numaproj.numaflow.examples.sideinput.Config;
import io.numaproj.numaflow.sideinput.Message;
import io.numaproj.numaflow.sideinput.Server;
import io.numaproj.numaflow.sideinput.SideInputRetriever;
import lombok.extern.slf4j.Slf4j;

/**
 * This is a simple side input example.
 * Example shows how to broadcast a message to other side input vertices.
 * This will be invoked for every fixed interval of time(defined in the pipeline config).
 * We are using a simple config class to hold the source and sampling rate(side input).
 * we are incrementing the sampling rate by 0.01 for every broadcast.
 * And if the sampling rate is greater than 0.9, we will reset it to 0.5.
 * In case of failure to serialize the config, we will drop the message(we will not broadcast the message).
 */

@Slf4j
public class SimpleSideInput extends SideInputRetriever {
    private final Config config;
    private final ObjectMapper jsonMapper = new ObjectMapper();

    public SimpleSideInput(Config config) {
        this.config = config;
    }

    public static void main(String[] args) throws Exception {
        Server server = new Server(new SimpleSideInput(new Config("sampling", 0.5F)));

        // Start the server
        server.start();

        // wait for the server to shut down
        server.awaitTermination();
    }

    @Override
    public Message retrieveSideInput() {
        byte[] val;
        if (0.9 > config.getSampling()) {
            config.setSampling(0.5F);
        } else {
            config.setSampling(config.getSampling() + 0.01F);
        }
        try {
            val = jsonMapper.writeValueAsBytes(config);
            // broadcastMessage will broadcast the message to other side input vertices
            log.info("Broadcasting side input message: {}", new String(val));
            return Message.createBroadcastMessage(val);
        } catch (JsonProcessingException e) {
            // noBroadcastMessage will drop the message
            log.error("Failed to serialize config: {}", e.getMessage());
            return Message.createNoBroadcastMessage();
        }
    }
}
