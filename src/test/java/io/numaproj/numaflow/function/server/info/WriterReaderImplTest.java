package io.numaproj.numaflow.function.server.info;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class WriterReaderImplTest {
    private WriterReader underTest = new WriterReaderImpl(new ObjectMapper());

    @Test
    public void given_localEnvironment_when_getJavaSDKVersion_then_returnAValidVersion() {
        String got = this.underTest.getSDKVersion();
        assertNotEquals("", got);
        try {
            Integer.parseInt(got);
        } catch (NumberFormatException e) {
            fail("Expected java SDK version to be a valid integer.");
        }
    }

    @Test
    public void given_writeServerInfo_when_read_then_returnExactSame() {
        ServerInfo testServerInfo = new ServerInfo(
                ServerInfoConstants.TCP_PROTOCOL,
                ServerInfoConstants.LANGUAGE_JAVA,
                "11",
                new HashMap<>() {{
                    put("key1", "value1");
                    put("key2", "value2");
                }}
        );
        String testFilePath = "/var/tmp/test-path";
        try {
            this.underTest.write(testServerInfo, testFilePath);
            ServerInfo got = this.underTest.read(testFilePath);
            assertEquals(testServerInfo.getLanguage(), got.getLanguage());
            assertEquals(testServerInfo.getProtocol(), got.getProtocol());
            assertEquals(testServerInfo.getVersion(), got.getVersion());
            assertEquals(testServerInfo.getMetadata(), got.getMetadata());
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(e.getMessage());
            fail("Expected no exception.");
        }
    }
}
