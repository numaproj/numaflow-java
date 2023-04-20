package io.numaproj.numaflow.info;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class ServerInfoAccessorImplTest {
    private ServerInfoAccessor underTest = new ServerInfoAccessorImpl(new ObjectMapper());

    @Test
    public void given_localEnvironment_when_getNumaflowJavaSDKVersion_then_returnAValidVersion() {
        String got = this.underTest.getSDKVersion();
        assertTrue(got.contains("0."));
    }

    @Test
    public void given_writeServerInfo_when_read_then_returnExactSame() {
        ServerInfo testServerInfo = new ServerInfo(
                Protocol.TCP_PROTOCOL,
                Language.JAVA,
                "0.4.3",
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
        } catch (Exception e) {
            fail("Expected no exception.");
        }
    }
}
