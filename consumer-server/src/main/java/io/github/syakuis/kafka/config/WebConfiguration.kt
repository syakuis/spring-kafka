package io.github.syakuis.kafka.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

@Configuration
class WebConfiguration {
    @Bean
    @Autowired
    fun createJsonHttpMessageConverter(objectMapper: ObjectMapper): HttpMessageConverter<Any> {
        val mapper = objectMapper.copy()
        val jackson2HttpMessageConverter = MappingJackson2HttpMessageConverter()
        jackson2HttpMessageConverter.objectMapper = mapper
        return jackson2HttpMessageConverter
    }
}
