package io.github.syakuis.kafka.notify.producer

import io.github.syakuis.kafka.KafkaProperties
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.*

/**
 * @author Seok Kyun. Choi.
 * @since 2021-11-05
 */
@RestController
@RequestMapping("/notify/producer")
class NotifyRestController {
    private val log = LoggerFactory.getLogger(NotifyRestController::class.java)

    @Value("\${spring.kafka.bootstrap-servers}")
    lateinit var bootstrapServers: String

    @PostMapping("/basic")
    @ResponseStatus(HttpStatus.OK)
    fun basic() {
        val kafkaTemplate: KafkaTemplate<String, String> = KafkaTemplate(DefaultKafkaProducerFactory(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            )
        ))

        (1..3).forEach {
            kafkaTemplate.send(KafkaProperties.basicTopicName,"$it - 키가 없는 일반적인 메시지")
        }
    }

    @PostMapping("/keys/{key}")
    @ResponseStatus(HttpStatus.OK)
    fun keys(@PathVariable("key") key: Int = 0) {
        val aKey: Int = key % 3

        log.debug("aKey: {}", aKey)

        val kafkaTemplate: KafkaTemplate<Int, String> = KafkaTemplate(DefaultKafkaProducerFactory(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to IntegerSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            )
        ))


        (1..3).forEach {
            kafkaTemplate.send(KafkaProperties.keyTopicName, aKey, "$it - $aKey 입니다.")
        }
    }

    @PostMapping("/ack")
    @ResponseStatus(HttpStatus.OK)
    fun ack() {
        val kafkaTemplate: KafkaTemplate<String, String> = KafkaTemplate(DefaultKafkaProducerFactory(
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            )
        ))

        kafkaTemplate.send(KafkaProperties.ackTopicName, "ack", "ok")
    }
}