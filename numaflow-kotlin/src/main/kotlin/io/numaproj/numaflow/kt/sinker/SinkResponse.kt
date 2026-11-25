package io.numaproj.numaflow.kt.sinker

import io.numaproj.numaflow.shared.UserMetadata

sealed interface SinkResponse {
    val id: String

    data class Ok(override val id: String) : SinkResponse
    data class Failure(override val id: String, val error: String) : SinkResponse
    data class Fallback(override val id: String) : SinkResponse

    data class Serve(override val id: String, val data: ByteArray) : SinkResponse {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Serve) return false
            return id == other.id && data.contentEquals(other.data)
        }

        override fun hashCode(): Int = 31 * id.hashCode() + data.contentHashCode()
    }

    data class OnSuccess(override val id: String, val message: SinkMessage? = null) : SinkResponse
}

data class SinkMessage(
    val value: ByteArray,
    val keys: List<String> = emptyList(),
    val userMetadata: UserMetadata? = null,
) {
    companion object {
        fun fromDatum(datum: SinkDatum): SinkMessage = SinkMessage(
            value = datum.value.clone(),
            keys = datum.keys,
            userMetadata = datum.userMetadata?.let { UserMetadata(it) },
        )
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SinkMessage) return false
        return value.contentEquals(other.value) && keys == other.keys
    }

    override fun hashCode(): Int = 31 * value.contentHashCode() + keys.hashCode()
}
