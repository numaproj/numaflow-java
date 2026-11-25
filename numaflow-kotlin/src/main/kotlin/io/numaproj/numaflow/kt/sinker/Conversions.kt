package io.numaproj.numaflow.kt.sinker

import io.numaproj.numaflow.shared.UserMetadata
import io.numaproj.numaflow.sinker.Message as JavaMessage
import io.numaproj.numaflow.sinker.Response as JavaResponse

internal fun SinkResponse.toJava(): JavaResponse = when (this) {
    is SinkResponse.Ok -> JavaResponse.responseOK(id)
    is SinkResponse.Failure -> JavaResponse.responseFailure(id, error)
    is SinkResponse.Fallback -> JavaResponse.responseFallback(id)
    is SinkResponse.Serve -> JavaResponse.responseServe(id, data)
    is SinkResponse.OnSuccess -> JavaResponse.responseOnSuccess(id, message?.toJava())
}

internal fun SinkMessage.toJava(): JavaMessage = JavaMessage(
    value,
    keys.toTypedArray(),
    userMetadata ?: UserMetadata(),
)

internal fun JavaResponse.toKotlin(): SinkResponse {
    return when {
        success == true -> SinkResponse.Ok(id)
        fallback == true -> SinkResponse.Fallback(id)
        serve == true -> SinkResponse.Serve(id, serveResponse ?: byteArrayOf())
        onSuccess == true -> SinkResponse.OnSuccess(
            id,
            onSuccessMessage?.let { msg ->
                SinkMessage(
                    value = msg.value ?: byteArrayOf(),
                    keys = msg.keys?.toList() ?: emptyList(),
                    userMetadata = msg.userMetadata,
                )
            },
        )
        else -> SinkResponse.Failure(id, err ?: "unknown error")
    }
}
