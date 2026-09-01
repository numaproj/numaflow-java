package io.numaproj.numaflow.kt.sinker

import io.numaproj.numaflow.shared.SystemMetadata
import io.numaproj.numaflow.shared.UserMetadata
import io.numaproj.numaflow.sinker.Datum
import java.time.Instant

data class SinkDatum(
    val id: String,
    val keys: List<String>,
    val value: ByteArray,
    val eventTime: Instant,
    val watermark: Instant,
    val headers: Map<String, String>,
    val userMetadata: UserMetadata?,
    val systemMetadata: SystemMetadata?,
) {
    companion object {
        fun from(datum: Datum): SinkDatum = SinkDatum(
            id = datum.id ?: "",
            keys = datum.keys?.toList() ?: emptyList(),
            value = datum.value?.clone() ?: byteArrayOf(),
            eventTime = datum.eventTime ?: Instant.EPOCH,
            watermark = datum.watermark ?: Instant.EPOCH,
            headers = datum.headers ?: emptyMap(),
            userMetadata = datum.userMetadata,
            systemMetadata = datum.systemMetadata,
        )
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SinkDatum) return false
        return id == other.id &&
            keys == other.keys &&
            value.contentEquals(other.value) &&
            eventTime == other.eventTime &&
            watermark == other.watermark &&
            headers == other.headers
    }

    override fun hashCode(): Int {
        var result = id.hashCode()
        result = 31 * result + keys.hashCode()
        result = 31 * result + value.contentHashCode()
        result = 31 * result + eventTime.hashCode()
        result = 31 * result + watermark.hashCode()
        result = 31 * result + headers.hashCode()
        return result
    }
}
