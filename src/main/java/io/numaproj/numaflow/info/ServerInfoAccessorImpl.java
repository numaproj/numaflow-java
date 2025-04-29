package io.numaproj.numaflow.info;

import static com.fasterxml.jackson.core.JsonGenerator.Feature.AUTO_CLOSE_TARGET;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class ServerInfoAccessorImpl implements ServerInfoAccessor {
  private static final String INFO_EOF = "U+005C__END__";
  private ObjectMapper objectMapper;

  @Override
  public String getSDKVersion() {
    try (InputStream in =
        getClass().getClassLoader().getResourceAsStream("numaflow-java-sdk-version.properties")) {
      if (in == null) {
        log.warn("numaflow-java-sdk-version.properties not found in classpath");
        return "";
      }
      Properties properties = new Properties();
      properties.load(in);
      return properties.getProperty("sdk.version", "");
    } catch (IOException e) {
      log.error("Error reading numaflow-java-sdk-version.properties", e);
      return "";
    }
  }

  @Override
  public void write(ServerInfo serverInfo, String filePath) throws Exception {
    try (FileWriter fileWriter = new FileWriter(filePath);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
      // Turn off AUTO_CLOSE_TARGET to prevent closing the underlying stream
      // when the ObjectMapper is closed.
      // We will let the FileWriter and BufferedWriter handle the closing.
      objectMapper.configure(AUTO_CLOSE_TARGET, false);
      objectMapper.writeValue(bufferedWriter, serverInfo);
      bufferedWriter.write(INFO_EOF);
    }
  }

  @Override
  public ServerInfo read(String filePath) throws Exception {
    String content = Files.readString(Path.of(filePath));
    String trimmedContent = verifyEOFAtEndAndTrim(content);
    return objectMapper.readValue(trimmedContent, ServerInfo.class);
  }

  private String verifyEOFAtEndAndTrim(String content) throws Exception {
    int eofIndex = content.lastIndexOf(INFO_EOF);
    if (eofIndex == -1) {
      throw new Exception("EOF marker not found in the file content");
    }
    if (eofIndex != content.length() - INFO_EOF.length()) {
      throw new Exception("EOF marker is not at the end of the file content");
    }
    return content.substring(0, eofIndex);
  }
}
