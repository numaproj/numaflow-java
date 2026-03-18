package io.numaproj.numaflow.kt.sinker

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList

suspend fun Flow<SinkDatum>.processEach(block: suspend (SinkDatum) -> SinkResponse): List<SinkResponse> =
    map { block(it) }.toList()

fun SinkDatum.ok(): SinkResponse.Ok = SinkResponse.Ok(id)

fun SinkDatum.failure(error: String): SinkResponse.Failure = SinkResponse.Failure(id, error)

fun SinkDatum.fallback(): SinkResponse.Fallback = SinkResponse.Fallback(id)

fun SinkDatum.valueAsString(): String = String(value)
