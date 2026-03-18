package io.numaproj.numaflow.kt.sinker

import io.numaproj.numaflow.sinker.GRPCConfig
import io.numaproj.numaflow.sinker.Server
import kotlinx.coroutines.flow.Flow

class SinkServerConfig {
    var socketPath: String? = null
    var maxMessageSize: Int? = null
    var port: Int? = null
    var isLocal: Boolean? = null
    var infoFilePath: String? = null

    internal fun toGrpcConfig(): GRPCConfig {
        val builder = GRPCConfig.newBuilder()
        socketPath?.let { builder.socketPath(it) }
        maxMessageSize?.let { builder.maxMessageSize(it) }
        port?.let { builder.port(it) }
        isLocal?.let { builder.isLocal(it) }
        infoFilePath?.let { builder.infoFilePath(it) }
        return builder.build()
    }
}

class SinkServer internal constructor(private val javaServer: Server) {
    fun start() { javaServer.start() }
    fun awaitTermination() { javaServer.awaitTermination() }
    fun stop() { javaServer.stop() }

    fun run() {
        start()
        awaitTermination()
    }
}

fun sinkServer(config: SinkServerConfig.() -> Unit = {}, handler: SinkHandler): SinkServer {
    val cfg = SinkServerConfig().apply(config)
    val bridge = SinkerBridge(handler)
    val javaServer = if (cfg.socketPath != null || cfg.maxMessageSize != null ||
        cfg.port != null || cfg.isLocal != null || cfg.infoFilePath != null
    ) {
        Server(bridge, cfg.toGrpcConfig())
    } else {
        Server(bridge)
    }
    return SinkServer(javaServer)
}

fun sinkServer(handler: suspend (Flow<SinkDatum>) -> List<SinkResponse>): SinkServer {
    return sinkServer(handler = SinkHandler { datums -> handler(datums) })
}
