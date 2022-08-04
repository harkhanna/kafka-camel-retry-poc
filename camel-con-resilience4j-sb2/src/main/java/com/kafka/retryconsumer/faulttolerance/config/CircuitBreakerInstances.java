package com.kafka.retryconsumer.faulttolerance.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClientException;

import java.time.Duration;

@Configuration
@AllArgsConstructor
@Slf4j
public class CircuitBreakerInstances {

    public static final String CIRCUIT_BREAKER_INSTANCE_TOPIC_1 = "circuit-breaker-instance-topic1";
    public static final String CIRCUIT_BREAKER_INSTANCE_TOPIC_2 = "circuit-breaker-instance-topic2";

    private final CircuitBreakerRegistry circuitBreakerRegistry;

    @Bean
    public CircuitBreakerConfig defaultCircuitBreakerConfig() {
        return CircuitBreakerConfig
                .custom()
                .failureRateThreshold(60)
                .waitDurationInOpenState(Duration.ofSeconds(20))
                .minimumNumberOfCalls(5)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10)
                .recordExceptions(RestClientException.class)
                .build();
    }

    @Bean
    public CircuitBreaker circuitBreakerInstanceTopic1(CircuitBreakerConfig defaultCircuitBreakerConfig) {
        var cb = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_INSTANCE_TOPIC_1, defaultCircuitBreakerConfig);
        cb.getEventPublisher().onStateTransition(event ->
                log.info("{} circuit breaker: {}", event.getCircuitBreakerName(), event.getStateTransition()));
        return cb;
    }

    @Bean
    public CircuitBreaker circuitBreakerInstanceTopic2(CircuitBreakerConfig defaultCircuitBreakerConfig) {
        var cb = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_INSTANCE_TOPIC_2, defaultCircuitBreakerConfig);
        cb.getEventPublisher().onStateTransition(event ->
                log.info("{} circuit breaker: {}", event.getCircuitBreakerName(), event.getStateTransition()));
        return cb;
    }
}
