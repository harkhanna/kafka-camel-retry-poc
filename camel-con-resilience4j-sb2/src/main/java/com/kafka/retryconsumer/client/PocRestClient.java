package com.kafka.retryconsumer.client;

import com.kafka.retryconsumer.faulttolerance.config.CircuitBreakerInstances;
import com.kafka.retryconsumer.faulttolerance.config.RetryInstances;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;

@Service
@Slf4j
public class PocRestClient {

    private static final String SERVICE_URL = "http://localhost:6060/?message=";

    @CircuitBreaker(name = CircuitBreakerInstances.CIRCUIT_BREAKER_INSTANCE_TOPIC_1)
    public void restClient1(Exchange exchange) {
        internalRestClient(exchange.getIn().getBody().toString());
    }

    @CircuitBreaker(name = CircuitBreakerInstances.CIRCUIT_BREAKER_INSTANCE_TOPIC_2)
    @Retry(name = RetryInstances.RETRY_INSTANCE_TOPIC_2)
    public void restClient2(Exchange exchange) {
        internalRestClient(exchange.getIn().getBody().toString());
    }

    public void internalRestClient(String message) {
        log.debug("rest call...");
        log.debug(" Making a request to {} at :{}", SERVICE_URL + message, LocalDateTime.now());
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.getForObject(SERVICE_URL, String.class);
    }
}
