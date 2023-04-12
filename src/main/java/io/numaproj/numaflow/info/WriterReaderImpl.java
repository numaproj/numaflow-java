package io.numaproj.numaflow.info;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

// TODO - DOC
@AllArgsConstructor
public class WriterReaderImpl implements WriterReader {
    private ObjectMapper objectMapper;

    @Override
    public String getSDKVersion() {
        // This only works for Java 9 and above.
        // Since we already use 11+ for numaflow SDK, it's safe to apply this approach.
        return String.valueOf(Runtime.version().version().get(0));
    }

    @Override
    public void write(ServerInfo serverInfo, String filePath) throws IOException {
        File file = new File(filePath);
        if (file.exists()) {
            file.delete();
        }
        FileWriter fileWriter = new FileWriter(filePath, false);
        objectMapper.writeValue(fileWriter, serverInfo);
        FileWriter eofWriter = new FileWriter(filePath, true);
        eofWriter.append("\n");
        eofWriter.append(ServerInfoConstants.EOF);
        eofWriter.close();
        fileWriter.close();
    }

    @Override
    public ServerInfo read(String filePath) throws IOException {
        File file = new File(filePath);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        StringBuilder stringBuilder = new StringBuilder();
        String line = bufferedReader.readLine();
        while (line != null && !line.equals(ServerInfoConstants.EOF)) {
            stringBuilder.append(line);
            line = bufferedReader.readLine();
        }
        ServerInfo serverInfo = objectMapper.readValue(stringBuilder.toString(), ServerInfo.class);
        bufferedReader.close();
        return serverInfo;
    }
}
