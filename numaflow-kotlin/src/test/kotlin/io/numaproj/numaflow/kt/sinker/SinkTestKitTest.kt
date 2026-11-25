package io.numaproj.numaflow.kt.sinker

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertIs

class SinkTestKitTest {

    @Test
    fun `datum factory creates SinkDatum with defaults`() {
        val datum = SinkTestKit.datum(id = "1")
        assertEquals("1", datum.id)
        assertEquals(emptyList(), datum.keys)
        assertEquals(0, datum.value.size)
        assertEquals(emptyMap(), datum.headers)
    }

    @Test
    fun `datum factory creates SinkDatum with custom values`() {
        val now = Instant.now()
        val datum = SinkTestKit.datum(
            id = "2",
            value = "payload".toByteArray(),
            keys = listOf("k1", "k2"),
            eventTime = now,
            watermark = now,
            headers = mapOf("header" to "value"),
        )
        assertEquals("2", datum.id)
        assertEquals(listOf("k1", "k2"), datum.keys)
        assertEquals("payload", String(datum.value))
        assertEquals(now, datum.eventTime)
        assertEquals(mapOf("header" to "value"), datum.headers)
    }

    @Test
    fun `test invokes handler directly with Flow`() = runTest {
        val handler = SinkHandler { datums ->
            datums.processEach { it.ok() }
        }

        val results = SinkTestKit.test(
            handler,
            listOf(
                SinkTestKit.datum(id = "1", value = "hello".toByteArray()),
                SinkTestKit.datum(id = "2", value = "world".toByteArray()),
            ),
        )

        assertEquals(2, results.size)
        assertEquals(SinkResponse.Ok("1"), results[0])
        assertEquals(SinkResponse.Ok("2"), results[1])
    }

    @Test
    fun `test handles mixed response types`() = runTest {
        val handler = SinkHandler { datums ->
            datums.processEach { datum ->
                if (datum.valueAsString() == "good") datum.ok()
                else datum.failure("bad data")
            }
        }

        val results = SinkTestKit.test(
            handler,
            listOf(
                SinkTestKit.datum(id = "1", value = "good".toByteArray()),
                SinkTestKit.datum(id = "2", value = "bad".toByteArray()),
            ),
        )

        assertIs<SinkResponse.Ok>(results[0])
        assertIs<SinkResponse.Failure>(results[1])
        assertEquals("bad data", (results[1] as SinkResponse.Failure).error)
    }
}
