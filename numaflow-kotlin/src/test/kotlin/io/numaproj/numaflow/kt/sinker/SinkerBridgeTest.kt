package io.numaproj.numaflow.kt.sinker

import io.numaproj.numaflow.sinker.DatumIterator
import io.numaproj.numaflow.sinker.SinkerTestKit
import kotlinx.coroutines.flow.toList
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals

class SinkerBridgeTest {

    @Test
    fun `bridge delegates to handler and returns correct ResponseList`() {
        val handler = SinkHandler { datums ->
            datums.toList().map { SinkResponse.Ok(it.id) }
        }
        val bridge = SinkerBridge(handler)

        val iterator = SinkerTestKit.TestListIterator()
        iterator.addDatum(
            SinkerTestKit.TestDatum.builder()
                .id("msg-1")
                .value("hello".toByteArray())
                .keys(arrayOf("k1"))
                .eventTime(Instant.now())
                .watermark(Instant.now())
                .headers(emptyMap())
                .build()
        )
        iterator.addDatum(
            SinkerTestKit.TestDatum.builder()
                .id("msg-2")
                .value("world".toByteArray())
                .keys(arrayOf("k2"))
                .eventTime(Instant.now())
                .watermark(Instant.now())
                .headers(emptyMap())
                .build()
        )

        val result = bridge.processMessages(iterator)

        assertEquals(2, result.responses.size)
        assertEquals("msg-1", result.responses[0].id)
        assertEquals(true, result.responses[0].success)
        assertEquals("msg-2", result.responses[1].id)
    }

    @Test
    fun `bridge handles failure responses`() {
        val handler = SinkHandler { datums ->
            datums.toList().map { SinkResponse.Failure(it.id, "failed") }
        }
        val bridge = SinkerBridge(handler)

        val iterator = SinkerTestKit.TestListIterator()
        iterator.addDatum(
            SinkerTestKit.TestDatum.builder()
                .id("msg-1")
                .value("data".toByteArray())
                .keys(emptyArray())
                .eventTime(Instant.now())
                .watermark(Instant.now())
                .headers(emptyMap())
                .build()
        )

        val result = bridge.processMessages(iterator)

        assertEquals(1, result.responses.size)
        assertEquals(false, result.responses[0].success)
        assertEquals("failed", result.responses[0].err)
    }
}
