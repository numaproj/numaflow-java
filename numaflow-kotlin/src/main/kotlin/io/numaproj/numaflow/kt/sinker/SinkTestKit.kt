package io.numaproj.numaflow.kt.sinker

import io.numaproj.numaflow.sinker.GRPCConfig
import io.numaproj.numaflow.sinker.SinkerTestKit
import kotlinx.coroutines.flow.asFlow
import java.time.Instant

object SinkTestKit {

    fun datum(
        id: String,
        value: ByteArray = byteArrayOf(),
        keys: List<String> = emptyList(),
        eventTime: Instant = Instant.now(),
        watermark: Instant = Instant.now(),
        headers: Map<String, String> = emptyMap(),
    ): SinkDatum = SinkDatum(
        id = id,
        keys = keys,
        value = value,
        eventTime = eventTime,
        watermark = watermark,
        headers = headers,
        userMetadata = null,
        systemMetadata = null,
    )

    suspend fun test(handler: SinkHandler, datums: List<SinkDatum>): List<SinkResponse> =
        handler.processMessages(datums.asFlow())

    fun grpcTest(handler: SinkHandler): GrpcTestHarness = GrpcTestHarness(handler)

    class GrpcTestHarness(private val handler: SinkHandler) {
        private val testKit = SinkerTestKit(
            SinkerBridge(handler),
            GRPCConfig.newBuilder().isLocal(true).build(),
        )

        fun start() { testKit.startServer() }
        fun stop() { testKit.stopServer() }

        fun client(): SinkerTestKit.Client = SinkerTestKit.Client()

        fun sendRequest(datums: List<SinkDatum>): List<SinkResponse> {
            val iterator = SinkerTestKit.TestListIterator()
            for (datum in datums) {
                iterator.addDatum(datum.toTestDatum())
            }
            val client = client()
            try {
                val responseList = client.sendRequest(iterator)
                return responseList.responses.map { it.toKotlin() }
            } finally {
                client.close()
            }
        }

        private fun SinkDatum.toTestDatum(): SinkerTestKit.TestDatum =
            SinkerTestKit.TestDatum.builder()
                .id(id)
                .keys(keys.toTypedArray())
                .value(value)
                .eventTime(eventTime)
                .watermark(watermark)
                .headers(headers)
                .build()
    }
}
