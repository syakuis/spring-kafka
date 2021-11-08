package io.github.syakuis.kafka

/**
 * @author Seok Kyun. Choi.
 * @since 2021-11-08
 */
data class MessagePayload(
    val message: String = "",
    val count: Int = 0
)