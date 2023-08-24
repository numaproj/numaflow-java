package io.numaproj.numaflow.examples.sideinput.simple;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.numaproj.numaflow.sideinput.Message;
import io.numaproj.numaflow.sideinput.Server;
import io.numaproj.numaflow.sideinput.SideInputRetriever;

public class SimpleSideInput extends SideInputRetriever {
    private final Config config;
    private ObjectMapper jsonMapper = new ObjectMapper();

    public SimpleSideInput(Config config) {
        this.config = config;
    }

    @Override
    public Message retrieveSideInput() {
        byte[] val;
        if (0.9 > config.getDropRatio()) {
            try {
                val = jsonMapper.writeValueAsBytes(config);
            } catch (JsonProcessingException e) {
                return new Message(new byte[0]);
            }
            return new Message(val);
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        new Server(new SimpleSideInput(new Config("sampling", 0.5F))).start();
    }
}
