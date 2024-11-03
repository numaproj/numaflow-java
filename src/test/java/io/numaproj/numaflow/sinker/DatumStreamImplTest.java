package io.numaproj.numaflow.sinker;

import lombok.AllArgsConstructor;
import org.junit.Test;

import java.time.Instant;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DatumStreamImplTest {
    @Test
    public void datumStreamNextTest() throws InterruptedException {
        Datum datum1 = new TestDatum("test1");
        Datum datum2 = new TestDatum("test2");
        Datum eofDatum = HandlerDatum.EOF_DATUM;

        DatumIteratorImpl datumIterator = new DatumIteratorImpl();

        datumIterator.write(datum1);
        datumIterator.write(datum2);
        datumIterator.write(eofDatum);

        assertEquals(datum1, datumIterator.next());

        assertEquals(datum2, datumIterator.next());


        assertNull(datumIterator.next());
    }

    @AllArgsConstructor
    public static class TestDatum implements Datum {
        private String id;

        @Override
        public String[] getKeys() {
            return new String[0];
        }

        @Override
        public byte[] getValue() {
            return new byte[0];
        }

        @Override
        public Instant getEventTime() {
            return null;
        }

        @Override
        public Instant getWatermark() {
            return null;
        }

        @Override
        public String getId() {
            return null;
        }

        @Override
        public Map<String, String> getHeaders() {
            return null;
        }
    }

}
