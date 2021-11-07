package io.github.syakuis.kafka.notify.consumer

import io.github.syakuis.kafka.KafkaProperties
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.TopicPartition
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Service

/**
 * @author Seok Kyun. Choi.
 * @since 2021-11-07
 */
@Service
class NotifyConsumerService {
    private val log = LoggerFactory.getLogger(NotifyConsumerService::class.java)
    @KafkaListener(topicPartitions = [TopicPartition(topic = KafkaProperties.topicName, partitions = ["0"])])
    fun receive(@Payload message: String) {
        log.debug("{}", message)
    }
}