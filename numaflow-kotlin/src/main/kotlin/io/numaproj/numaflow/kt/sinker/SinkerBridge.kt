package io.numaproj.numaflow.kt.sinker

import io.numaproj.numaflow.sinker.DatumIterator
import io.numaproj.numaflow.sinker.ResponseList
import io.numaproj.numaflow.sinker.Sinker
import kotlinx.coroutines.runBlocking

internal class SinkerBridge(private val handler: SinkHandler) : Sinker() {
    override fun processMessages(datumStream: DatumIterator): ResponseList {
        val responses = runBlocking {
            handler.processMessages(datumStream.asFlow())
        }
        val builder = ResponseList.newBuilder()
        for (response in responses) {
            builder.addResponse(response.toJava())
        }
        return builder.build()
    }
}
