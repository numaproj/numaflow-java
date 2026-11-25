package io.numaproj.numaflow.kt.sinker

import io.numaproj.numaflow.sinker.DatumIterator
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn

internal fun DatumIterator.asFlow(): Flow<SinkDatum> = flow {
    while (true) {
        val datum = next() ?: break
        emit(SinkDatum.from(datum))
    }
}.flowOn(Dispatchers.IO)
