package io.numaproj.numaflow.kt.sinker

import kotlinx.coroutines.flow.toList
import org.junit.jupiter.api.Test
import kotlin.test.assertNotNull

class SinkServerTest {

    @Test
    fun `sinkServer with handler creates server`() {
        val server = sinkServer(handler = SinkHandler { datums ->
            datums.toList().map { SinkResponse.Ok(it.id) }
        })
        assertNotNull(server)
    }

    @Test
    fun `sinkServer with lambda creates server`() {
        val server = sinkServer { datums ->
            datums.toList().map { SinkResponse.Ok(it.id) }
        }
        assertNotNull(server)
    }

    @Test
    fun `sinkServer with config creates server`() {
        val server = sinkServer(
            config = {
                isLocal = true
                port = 50052
                maxMessageSize = 1024 * 1024 * 8
            },
            handler = SinkHandler { datums ->
                datums.toList().map { SinkResponse.Ok(it.id) }
            },
        )
        assertNotNull(server)
    }

    @Test
    fun `SinkServerConfig toGrpcConfig applies values`() {
        val cfg = SinkServerConfig().apply {
            socketPath = "/tmp/test.sock"
            maxMessageSize = 999
            port = 12345
            isLocal = true
            infoFilePath = "/tmp/info"
        }
        val grpc = cfg.toGrpcConfig()
        assertNotNull(grpc)
    }
}
