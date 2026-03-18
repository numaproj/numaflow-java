package io.numaproj.numaflow.kt.sinker

import io.numaproj.numaflow.sinker.DatumIterator
import io.numaproj.numaflow.sinker.SinkerTestKit
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals

class FlowAdapterTest {

    @Test
    fun `asFlow converts iterator elements to SinkDatum`() = runTest {
        val now = Instant.now()
        val iterator = SinkerTestKit.TestListIterator()
        iterator.addDatum(
            SinkerTestKit.TestDatum.builder()
                .id("1")
                .value("a".toByteArray())
                .keys(arrayOf("k"))
                .eventTime(now)
                .watermark(now)
                .headers(mapOf("h" to "v"))
                .build()
        )
        iterator.addDatum(
            SinkerTestKit.TestDatum.builder()
                .id("2")
                .value("b".toByteArray())
                .keys(emptyArray())
                .eventTime(now)
                .watermark(now)
                .headers(emptyMap())
                .build()
        )

        val results = iterator.asFlow().toList()

        assertEquals(2, results.size)
        assertEquals("1", results[0].id)
        assertEquals("a", String(results[0].value))
        assertEquals(listOf("k"), results[0].keys)
        assertEquals("2", results[1].id)
    }

    @Test
    fun `asFlow handles empty iterator`() = runTest {
        val iterator = SinkerTestKit.TestListIterator()

        val results = iterator.asFlow().toList()

        assertEquals(0, results.size)
    }

    @Test
    fun `asFlow stops at null (EOF)`() = runTest {
        val iterator = object : DatumIterator {
            private var count = 0
            override fun next() = if (count++ < 3) {
                SinkerTestKit.TestDatum.builder()
                    .id("msg-$count")
                    .value(byteArrayOf())
                    .keys(emptyArray())
                    .eventTime(Instant.now())
                    .watermark(Instant.now())
                    .headers(emptyMap())
                    .build()
            } else null
        }

        val results = iterator.asFlow().toList()
        assertEquals(3, results.size)
    }
}
