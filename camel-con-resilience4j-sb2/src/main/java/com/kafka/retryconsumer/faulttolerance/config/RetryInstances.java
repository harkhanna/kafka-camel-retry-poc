package com.kafka.retryconsumer.faulttolerance.config;

import com.kafka.retryconsumer.faulttolerance.retry.backoff.FixedBackOffPolicyWithCb;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.web.client.RestClientException;

import java.time.Duration;
import java.util.Optional;

@Configuration
@AllArgsConstructor
@Slf4j
public class RetryInstances {

    public static final String RETRY_INSTANCE_TOPIC_2 = "retry-instance-topic2";
    private final RetryRegistry retryRegistry;
    private final CircuitBreaker circuitBreakerInstanceTopic1;
    private final CircuitBreaker circuitBreakerInstanceTopic2;

    public RetryConfig defaultRetryConfig(CircuitBreaker circuitBreaker) {
        return RetryConfig
                .custom()
                .maxAttempts(10)
                .retryExceptions(RestClientException.class,
                        CallNotPermittedException.class)
                .intervalBiFunction(
                        (integer, objects) -> {
                            long duration = Duration.ofSeconds(1).toMillis();
                            long cbDuration = Optional.ofNullable(circuitBreaker)
                                    .filter(cb -> !cb.tryAcquirePermission())
                                    .map(cb -> cb.getCircuitBreakerConfig()
                                            .getWaitIntervalFunctionInOpenState().apply(integer) + 1000L)
                                    .orElse(0L);
                            final var maxDuration = Math.max(duration, cbDuration);
                            log.debug("retry back-off: {}, {}ms", integer, maxDuration);
                            return maxDuration;
                        }
                )
                .build();
    }

    @Bean
    public RetryTemplate retryInstanceTopic1() {
        return RetryTemplate.builder()
//                .infiniteRetry()
                .maxAttempts(10)
                .customBackoff(new FixedBackOffPolicyWithCb(Duration.ofSeconds(1),
                        circuitBreakerInstanceTopic1))
                .build();
    }

    @Bean
    public Retry retryInstanceTopic2() {
        RetryConfig retryConfig = defaultRetryConfig(circuitBreakerInstanceTopic2);
        return retryRegistry.retry(RETRY_INSTANCE_TOPIC_2, retryConfig);
    }
}
