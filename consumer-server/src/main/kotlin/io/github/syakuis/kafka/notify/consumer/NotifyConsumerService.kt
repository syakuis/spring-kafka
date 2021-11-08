package io.github.syakuis.kafka.notify.consumer

import io.github.syakuis.kafka.KafkaProperties
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.TopicPartition
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service

/**
 * @author Seok Kyun. Choi.
 * @since 2021-11-07
 */
@Service
class NotifyConsumerService {
    private val log = LoggerFactory.getLogger(NotifyConsumerService::class.java)
    @KafkaListener(id = "notify", topicPartitions = [TopicPartition(topic = KafkaProperties.topicName, partitions = ["0"])])
    fun receive(@Payload message: String,
                @Header(KafkaHeaders.GROUP_ID) groupId: String,
                @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partitionId: Int,
                @Header(KafkaHeaders.OFFSET) offset: Int
    ) {
        log.debug("groupId: {}, partition: {}, offset: {} -> {}", groupId, partitionId, offset, message)
    }
}