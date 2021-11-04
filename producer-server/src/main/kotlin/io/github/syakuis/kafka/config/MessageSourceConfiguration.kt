package io.github.syakuis.kafka.config;

import org.springframework.context.MessageSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.support.MessageSourceAccessor
import java.util.*

@Configuration
class MessageSourceConfiguration {
    @Bean("i18n")
    fun i18n(messageSource: MessageSource): MessageSourceAccessor {
        return MessageSourceAccessor(messageSource, Locale.getDefault())
    }
}
