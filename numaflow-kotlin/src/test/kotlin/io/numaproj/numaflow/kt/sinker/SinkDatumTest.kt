package io.numaproj.numaflow.kt.sinker

import io.numaproj.numaflow.shared.UserMetadata
import io.numaproj.numaflow.sinker.Datum
import io.numaproj.numaflow.shared.SystemMetadata
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class SinkDatumTest {

    @Test
    fun `from converts Java Datum correctly`() {
        val now = Instant.now()
        val javaDatum = object : Datum {
            override fun getKeys() = arrayOf("key1", "key2")
            override fun getValue() = "hello".toByteArray()
            override fun getEventTime() = now
            override fun getWatermark() = now
            override fun getId() = "msg-1"
            override fun getHeaders() = mapOf("h1" to "v1")
            override fun getUserMetadata(): UserMetadata? = null
            override fun getSystemMetadata(): SystemMetadata? = null
        }

        val datum = SinkDatum.from(javaDatum)

        assertEquals("msg-1", datum.id)
        assertEquals(listOf("key1", "key2"), datum.keys)
        assertEquals("hello", String(datum.value))
        assertEquals(now, datum.eventTime)
        assertEquals(now, datum.watermark)
        assertEquals(mapOf("h1" to "v1"), datum.headers)
    }

    @Test
    fun `from handles null fields gracefully`() {
        val javaDatum = object : Datum {
            override fun getKeys(): Array<String>? = null
            override fun getValue(): ByteArray? = null
            override fun getEventTime(): Instant? = null
            override fun getWatermark(): Instant? = null
            override fun getId(): String? = null
            override fun getHeaders(): Map<String, String>? = null
            override fun getUserMetadata(): UserMetadata? = null
            override fun getSystemMetadata(): SystemMetadata? = null
        }

        val datum = SinkDatum.from(javaDatum)

        assertEquals("", datum.id)
        assertEquals(emptyList(), datum.keys)
        assertEquals(0, datum.value.size)
        assertEquals(Instant.EPOCH, datum.eventTime)
    }

    @Test
    fun `equals and hashCode handle ByteArray correctly`() {
        val now = Instant.now()
        val datum1 = SinkDatum(
            id = "1", keys = listOf("k"), value = "abc".toByteArray(),
            eventTime = now, watermark = now, headers = emptyMap(),
            userMetadata = null, systemMetadata = null,
        )
        val datum2 = SinkDatum(
            id = "1", keys = listOf("k"), value = "abc".toByteArray(),
            eventTime = now, watermark = now, headers = emptyMap(),
            userMetadata = null, systemMetadata = null,
        )
        val datum3 = SinkDatum(
            id = "1", keys = listOf("k"), value = "xyz".toByteArray(),
            eventTime = now, watermark = now, headers = emptyMap(),
            userMetadata = null, systemMetadata = null,
        )

        assertEquals(datum1, datum2)
        assertEquals(datum1.hashCode(), datum2.hashCode())
        assertNotEquals(datum1, datum3)
    }
}
