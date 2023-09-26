package io.numaproj.numaflow.sinker;

import lombok.AllArgsConstructor;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DatumStreamImplTest {
    @Test
    public void hasNextAndNextReturnCorrectElements() throws InterruptedException {
        Datum datum1 = new TestDatum("test1");
        Datum datum2 = new TestDatum("test2");
        Datum eofDatum = HandlerDatum.EOF_DATUM;

        DatumIteratorImpl datumIterator = new DatumIteratorImpl();

        datumIterator.writeMessage(datum1);
        datumIterator.writeMessage(datum2);
        datumIterator.writeMessage(eofDatum);

        assertTrue(datumIterator.hasNext());
        assertEquals(datum1, datumIterator.next());

        assertTrue(datumIterator.hasNext());
        assertEquals(datum2, datumIterator.next());

        assertFalse(datumIterator.hasNext());

        assertThrows(IllegalStateException.class, () -> datumIterator.next());
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
    }

}
