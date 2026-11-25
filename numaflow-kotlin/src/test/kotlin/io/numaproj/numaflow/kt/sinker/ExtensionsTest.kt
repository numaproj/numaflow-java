package io.numaproj.numaflow.kt.sinker

import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertIs

class ExtensionsTest {

    private val now = Instant.now()

    private fun testDatum(id: String, value: String = "") = SinkDatum(
        id = id, keys = listOf("k"), value = value.toByteArray(),
        eventTime = now, watermark = now, headers = emptyMap(),
        userMetadata = null, systemMetadata = null,
    )

    @Test
    fun `processEach maps each datum to a response`() = runTest {
        val flow = flowOf(testDatum("1"), testDatum("2"), testDatum("3"))
        val results = flow.processEach { it.ok() }

        assertEquals(3, results.size)
        results.forEachIndexed { i, r ->
            assertIs<SinkResponse.Ok>(r)
            assertEquals("${i + 1}", r.id)
        }
    }

    @Test
    fun `ok extension creates Ok response`() {
        val datum = testDatum("x")
        val response = datum.ok()
        assertEquals(SinkResponse.Ok("x"), response)
    }

    @Test
    fun `failure extension creates Failure response`() {
        val datum = testDatum("y")
        val response = datum.failure("oops")
        assertEquals(SinkResponse.Failure("y", "oops"), response)
    }

    @Test
    fun `fallback extension creates Fallback response`() {
        val datum = testDatum("z")
        val response = datum.fallback()
        assertEquals(SinkResponse.Fallback("z"), response)
    }

    @Test
    fun `valueAsString decodes ByteArray`() {
        val datum = testDatum("1", "hello world")
        assertEquals("hello world", datum.valueAsString())
    }

    @Test
    fun `processEach with empty flow`() = runTest {
        val results = flowOf<SinkDatum>().processEach { it.ok() }
        assertEquals(0, results.size)
    }
}
