package io.numaproj.numaflow.examples.sideinput.simple;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.numaproj.numaflow.sideinput.Message;
import io.numaproj.numaflow.sideinput.Server;
import io.numaproj.numaflow.sideinput.SideInputRetriever;

public class SimpleSideInput extends SideInputRetriever {
    private final Config config;
    private final ObjectMapper jsonMapper = new ObjectMapper();

    public SimpleSideInput(Config config) {
        this.config = config;
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
            return Message.broadcastMessage(val);
        } catch (JsonProcessingException e) {
            // noBroadcastMessage will drop the message
            return Message.noBroadcastMessage();
        }
    }

    public static void main(String[] args) throws Exception {
        new Server(new SimpleSideInput(new Config("sampling", 0.5F))).start();
    }
}
