package io.github.syakuis.kafka.config

import io.github.syakuis.kafka.KafkaProperties
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.TopicBuilder

/**
 * @author Seok Kyun. Choi.
 * @since 2021-11-05
 */
@Configuration(proxyBeanMethods = false)
@EnableKafka
class KafkaConfiguration {
    @Bean
    fun basicTopic(): NewTopic = TopicBuilder.name(KafkaProperties.basicTopicName).partitions(1).replicas(1).build()

    @Bean
    fun keyTopic(): NewTopic = TopicBuilder.name(KafkaProperties.keyTopicName).partitions(3).replicas(1).build()

    @Bean
    fun ackTopic(): NewTopic = TopicBuilder.name(KafkaProperties.keyTopicName).partitions(1).replicas(1).build()
}