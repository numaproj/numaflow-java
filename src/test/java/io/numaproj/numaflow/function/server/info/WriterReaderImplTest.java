package io.numaproj.numaflow.function.server.info;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;


@RunWith(JUnit4.class)
public class WriterReaderImplTest {
    private WriterReader underTest = new WriterReaderImpl();

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
}
