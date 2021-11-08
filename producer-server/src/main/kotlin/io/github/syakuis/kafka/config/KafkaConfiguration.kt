package io.github.syakuis.kafka.config

import io.github.syakuis.kafka.KafkaProperties
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory

/**
 * @author Seok Kyun. Choi.
 * @since 2021-11-05
 */
@Configuration(proxyBeanMethods = false)
@EnableKafka
class KafkaConfiguration {
    @Value("\${spring.kafka.bootstrap-servers}")
    lateinit var bootstrapServers: String

    @Bean
    fun topic(): NewTopic = TopicBuilder.name(KafkaProperties.topicName).partitions(1).replicas(1).build()

    @Bean
    fun producerFactory(): ProducerFactory<Int, String> = DefaultKafkaProducerFactory(
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to IntegerSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
        )
    )

    @Bean
    fun kafkaTemplate(producerFactory: ProducerFactory<Int, String>): KafkaTemplate<Int, String> =
        KafkaTemplate(producerFactory)
}