package io.github.syakuis.kafka.notify.consumer

import io.github.syakuis.kafka.KafkaProperties
import io.github.syakuis.kafka.MessagePayload
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.annotation.TopicPartition
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata
import org.springframework.kafka.support.Acknowledgment
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

    @KafkaListener(id = KafkaProperties.basicTopicName, topics = [ KafkaProperties.basicTopicName ], containerFactory = "basicKafkaListenerFactory")
    fun basicReceive(@Payload message: String,
                     @Header(KafkaHeaders.GROUP_ID) groupId: String,
                     @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY, required = false) key: String?,
                     @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partitionId: Int,
                     @Header(KafkaHeaders.OFFSET) offset: Int
    ) {
        log.debug("groupId: {}, partition: {}, key: {}, offset: {} -> {}", groupId, partitionId, key, offset, message)
    }

    @KafkaListener(id = KafkaProperties.keyTopicName, topicPartitions = [
        TopicPartition(topic = KafkaProperties.keyTopicName, partitions = [ "0", "1", "2" ])
    ], containerFactory = "keysKafkaListenerFactory")
    fun keysReceive(@Payload message: String,
                    @Header(KafkaHeaders.GROUP_ID) groupId: String,
                    @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partitionId: Int,
                    @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) key: Int,
                    @Header(KafkaHeaders.OFFSET) offset: Int
    ) {
        log.debug("groupId: {}, partition: {}, key: {}, offset: {} -> {}", groupId, partitionId, key, offset, message)
    }

    @KafkaListener(id = KafkaProperties.ackTopicName, topics = [ KafkaProperties.ackTopicName ], containerFactory = "ackKafkaListenerFactory")
    fun ackReceive(@Payload message: String,
                   metadata: ConsumerRecordMetadata,
                   acknowledgment: Acknowledgment
    ) {
        log.debug("topic: {}, partition: {}, offset: {} -> {}", metadata.topic(), metadata.partition(), metadata.offset(), message)
        acknowledgment.acknowledge()
//        acknowledgment.nack(5)
    }

    @KafkaListener(id = KafkaProperties.retryTopicName, containerFactory = "retryKafkaListenerFactory")
    fun retryReceive(@Payload message: MessagePayload) {
//        if (message.count > 1) {
            log.error("{}", message)
            throw IllegalArgumentException("오류 발생")
//        } else {
//            log.debug("{}", message)
//        }
    }
}