package io.numaproj.numaflow.kt.sinker

import kotlinx.coroutines.flow.Flow

fun interface SinkHandler {
    suspend fun processMessages(datums: Flow<SinkDatum>): List<SinkResponse>
}
