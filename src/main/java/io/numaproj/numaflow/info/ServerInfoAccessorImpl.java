package io.numaproj.numaflow.info;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

@AllArgsConstructor
public class ServerInfoAccessorImpl implements ServerInfoAccessor {
    private ObjectMapper objectMapper;

    @Override
    public String getSDKVersion() {
        // This only works for Java 9 and above.
        // Since we already use 11+ for numaflow SDK, it's safe to apply this approach.
        return String.valueOf(Runtime.version().version().get(0));
    }

    @Override
    public void write(ServerInfo serverInfo, String filePath) throws Exception {
        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }
        FileWriter fileWriter = new FileWriter(filePath, false);
        FileWriter eofWriter = new FileWriter(filePath, true);
        try {
            objectMapper.writeValue(fileWriter, serverInfo);
            eofWriter.append("\n").append(ServerInfoConstants.EOF);
        } finally {
            eofWriter.close();
            fileWriter.close();
        }
    }

    @Override
    public ServerInfo read(String filePath) throws Exception {
        FileReader fileReader = new FileReader(filePath);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        StringBuilder stringBuilder = new StringBuilder();
        String line;
        ServerInfo serverInfo;
        try {
            while ((line = bufferedReader.readLine()) != null
                    && !line.equals(ServerInfoConstants.EOF)) {
                stringBuilder.append(line);
            }
            serverInfo = objectMapper.readValue(stringBuilder.toString(), ServerInfo.class);
        } finally {
            fileReader.close();
            bufferedReader.close();
        }
        return serverInfo;
    }
}
