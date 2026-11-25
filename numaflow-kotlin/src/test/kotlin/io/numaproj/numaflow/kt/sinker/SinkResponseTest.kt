package io.numaproj.numaflow.kt.sinker

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotEquals

class SinkResponseTest {

    @Test
    fun `Ok variant properties`() {
        val ok = SinkResponse.Ok("id-1")
        assertEquals("id-1", ok.id)
        assertIs<SinkResponse.Ok>(ok)
    }

    @Test
    fun `Failure variant properties`() {
        val fail = SinkResponse.Failure("id-2", "bad data")
        assertEquals("id-2", fail.id)
        assertEquals("bad data", fail.error)
    }

    @Test
    fun `Fallback variant properties`() {
        val fb = SinkResponse.Fallback("id-3")
        assertEquals("id-3", fb.id)
    }

    @Test
    fun `Serve variant equals handles ByteArray`() {
        val s1 = SinkResponse.Serve("id-4", "data".toByteArray())
        val s2 = SinkResponse.Serve("id-4", "data".toByteArray())
        val s3 = SinkResponse.Serve("id-4", "other".toByteArray())
        assertEquals(s1, s2)
        assertEquals(s1.hashCode(), s2.hashCode())
        assertNotEquals(s1, s3)
    }

    @Test
    fun `OnSuccess variant with and without message`() {
        val os1 = SinkResponse.OnSuccess("id-5")
        assertEquals(null, os1.message)

        val msg = SinkMessage(value = "v".toByteArray(), keys = listOf("k"))
        val os2 = SinkResponse.OnSuccess("id-6", msg)
        assertEquals(msg, os2.message)
    }

    @Test
    fun `toJava round-trip for Ok`() {
        val ok = SinkResponse.Ok("id-1")
        val java = ok.toJava()
        assertEquals("id-1", java.id)
        assertEquals(true, java.success)
    }

    @Test
    fun `toJava round-trip for Failure`() {
        val fail = SinkResponse.Failure("id-2", "err")
        val java = fail.toJava()
        assertEquals("id-2", java.id)
        assertEquals(false, java.success)
        assertEquals("err", java.err)
    }

    @Test
    fun `toJava round-trip for Fallback`() {
        val fb = SinkResponse.Fallback("id-3")
        val java = fb.toJava()
        assertEquals(true, java.fallback)
    }

    @Test
    fun `toJava round-trip for Serve`() {
        val serve = SinkResponse.Serve("id-4", "data".toByteArray())
        val java = serve.toJava()
        assertEquals(true, java.serve)
        assertEquals("data", String(java.serveResponse))
    }

    @Test
    fun `toJava round-trip for OnSuccess`() {
        val os = SinkResponse.OnSuccess("id-5")
        val java = os.toJava()
        assertEquals(true, java.onSuccess)
    }

    @Test
    fun `toKotlin conversions`() {
        assertEquals(SinkResponse.Ok("1"), io.numaproj.numaflow.sinker.Response.responseOK("1").toKotlin())
        assertEquals(SinkResponse.Fallback("2"), io.numaproj.numaflow.sinker.Response.responseFallback("2").toKotlin())

        val fail = io.numaproj.numaflow.sinker.Response.responseFailure("3", "oops").toKotlin()
        assertIs<SinkResponse.Failure>(fail)
        assertEquals("oops", fail.error)
    }
}
