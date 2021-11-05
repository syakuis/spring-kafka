package io.github.syakuis.kafka.notify.producer

import io.github.syakuis.kafka.KafkaProperties
import org.springframework.http.HttpStatus
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.*

/**
 * @author Seok Kyun. Choi.
 * @since 2021-11-05
 */
@RestController
@RequestMapping("/notify")
class NotifyRestController(private val kafkaTemplate: KafkaTemplate<Int, String>) {
    @PostMapping("/basic/{key}")
    @ResponseStatus(HttpStatus.OK)
    fun message(@PathVariable("key") key: Int = 1) {
        kafkaTemplate.send(KafkaProperties.topicName, key, "aaaa")
    }
}