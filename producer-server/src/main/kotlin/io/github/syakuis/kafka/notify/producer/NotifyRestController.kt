package io.github.syakuis.kafka.notify.producer

import io.github.syakuis.kafka.KafkaProperties
import org.springframework.http.HttpStatus
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.*
import java.util.stream.Stream

/**
 * @author Seok Kyun. Choi.
 * @since 2021-11-05
 */
@RestController
@RequestMapping("/notify")
class NotifyRestController(private val kafkaTemplate: KafkaTemplate<Int, String>) {
    @PostMapping("/is-null-key")
    @ResponseStatus(HttpStatus.OK)
    fun isNullKey() {
        (1..10).forEach {
            kafkaTemplate.send(KafkaProperties.topicName, "$it 키가 없습니다.")
        }
    }

    @PostMapping("/is-exists-key/{key}")
    @ResponseStatus(HttpStatus.OK)
    fun isExistsKey(@PathVariable("key") key: Int = 0) {
        kafkaTemplate.send(KafkaProperties.topicName, key, "key 는 $key 입니다.")
    }
}