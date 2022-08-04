package com.kafka.retryconsumer.faulttolerance.retry.backoff;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.springframework.retry.backoff.BackOffInterruptedException;
import org.springframework.retry.backoff.Sleeper;
import org.springframework.retry.backoff.StatelessBackOffPolicy;
import org.springframework.retry.backoff.ThreadWaitSleeper;

import java.time.Duration;
import java.util.Optional;

public class FixedBackOffPolicyWithCb extends StatelessBackOffPolicy {
    private final CircuitBreaker circuitBreaker;
    private volatile long backOffPeriod = 1000L;
    private final Sleeper sleeper;

    public FixedBackOffPolicyWithCb(CircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
        this.sleeper = new ThreadWaitSleeper();
    }

    public FixedBackOffPolicyWithCb(Duration backOffPeriod, CircuitBreaker circuitBreaker) {
        this.backOffPeriod = backOffPeriod.toMillis();
        this.circuitBreaker = circuitBreaker;
        this.sleeper = new ThreadWaitSleeper();
    }

    public void setBackOffPeriod(long backOffPeriod) {
        this.backOffPeriod = backOffPeriod > 0L ? backOffPeriod : 1L;
    }

    public long getBackOffPeriod() {
        return this.backOffPeriod;
    }

    protected void doBackOff() throws BackOffInterruptedException {
        try {
            long cbWaitDuration = Optional.of(circuitBreaker)
                    .filter(cb -> !cb.tryAcquirePermission())
                    .map(cb -> cb.getCircuitBreakerConfig()
                            .getWaitIntervalFunctionInOpenState().apply(1) + 1000L)
                    .orElse(0L);
            long maxBackOffPeriod = Math.max(this.backOffPeriod, cbWaitDuration);
            this.sleeper.sleep(maxBackOffPeriod);
        } catch (InterruptedException exp) {
            throw new BackOffInterruptedException("Thread interrupted while sleeping", exp);
        }
    }

    public String toString() {
        return "FixedBackOffPolicy[backOffPeriod=" + this.backOffPeriod + "]";
    }
}