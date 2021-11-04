package io.github.syakuis.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.syakuis.kafka.config.support.SimpleObjectMapper
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class BasicBeanConfiguration {
    @Bean
    fun objectMapper(): ObjectMapper {
        return SimpleObjectMapper.of(ObjectMapper())
    }
}
