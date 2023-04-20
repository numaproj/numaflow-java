package io.numaproj.numaflow.info;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

@AllArgsConstructor
public class ServerInfoAccessorImpl implements ServerInfoAccessor {
    private ObjectMapper objectMapper;

    @Override
    public String getSDKVersion() {
        String version = "";
        try (InputStream in = getClass()
                .getClassLoader()
                .getResourceAsStream("version.properties")) {
            if (in != null) {
                Properties properties = new Properties();
                properties.load(in);
                version = properties.getProperty("sdk.version");
            }
        } catch (IOException e) {
            return version;
        }
        return version;
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
            eofWriter.append(ServerInfoConstants.EOF);
        } finally {
            eofWriter.close();
            fileWriter.close();
        }
    }

    @Override
    public ServerInfo read(String filePath) throws Exception {
        String content = Files.readString(Path.of(filePath));
        String trimmedContent = verifyEOFAtEndAndTrim(content);
        ServerInfo serverInfo = objectMapper.readValue(trimmedContent, ServerInfo.class);
        return serverInfo;
    }

    private String verifyEOFAtEndAndTrim(String content) throws Exception {
        int eofIndex = content.lastIndexOf(ServerInfoConstants.EOF);
        if (eofIndex == -1) {
            throw new Exception("EOF marker not found in the file content");
        }
        if (eofIndex != content.length() - ServerInfoConstants.EOF.length()) {
            throw new Exception("EOF marker is not at the end of the file content");
        }
        return content.substring(0, eofIndex);
    }
}
