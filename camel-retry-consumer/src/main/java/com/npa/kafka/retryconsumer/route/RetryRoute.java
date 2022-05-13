package com.npa.kafka.retryconsumer.route;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.concurrent.TimeoutException;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaManualCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class RetryRoute extends RouteBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(RetryRoute.class);

  @Override
  public void configure() {

    restConfiguration().host("localhost").port("9080");

    String topicUrl1 = buildKafkaUrl("retry_topic_1");
    String topicUrl2 = buildKafkaUrl("retry_topic_2");
    LOGGER.info("Kafka consumer URL 1 is : {}", topicUrl1);
    LOGGER.info("Kafka consumer URL 2 is : {}", topicUrl2);
    LOGGER.info("Start time is {}", LocalDateTime.now());

    onException(Exception.class)
        .log("Exception message is ${exception.message}")
        .maximumRedeliveries(2) // -1 for infinite retries.
        .redeliveryDelay(100)
        .useExponentialBackOff()
        .backOffMultiplier(2)
        .maximumRedeliveryDelay(
            1024) // To limit the maximum duration between retries so that exponential backoff does not go out of limits.
        //.useCollisionAvoidance() // random trigger
        .log("BEFORE exception commit")
        //.process(this::doManualCommit)
        .handled(true)
    ;

    from(topicUrl1)
        .routeId("R1")
        .throttle(100).timePeriodMillis(10000) //Default 1000 ms
        .process(this::dumpKafkaDetails)
        .log("before rest call 1")
        .circuitBreaker()
        .inheritErrorHandler(true)
        .resilience4jConfiguration().slidingWindowSize(10)
        .writableStackTraceEnabled(false).timeoutEnabled(true).timeoutDuration(1000)
        .minimumNumberOfCalls(5).waitDurationInOpenState(20).end()
        //.delay(1000)
        .to("rest:get:/sample/hello")
        //.onFallback().log("FALLBACK") // This will not push for retry and instead will continue to next step in pipeline
        .endCircuitBreaker()
        .process(this::doManualCommit)
        .log("end");

    /*from(topicUrl2)
        .routeId("R2")
        .process(exchange -> {
          LOGGER.info(this.dumpKafkaDetails(exchange));
        })
        .log("before rest call 2")
        .circuitBreaker()
        .inheritErrorHandler(true)
            .resilience4jConfiguration().slidingWindowSize(10)
            .writableStackTraceEnabled(false).timeoutEnabled(true).timeoutDuration(1000)
            .minimumNumberOfCalls(5).waitDurationInOpenState(20).end()
        .to("http://localhost:8000/not-found")
        .endCircuitBreaker()
        .process(this::doManualCommit)
        .log("end");*/
  }

  /*
  This will force a synchronous commit which will block until the commit is acknowledge on Kafka, or if it fails an exception is thrown.
  You can use an asynchronous commit as well by configuring the KafkaManualCommitFactory with the DefaultKafkaManualAsyncCommitFactory implementation.

  The commit will then be done in the next consumer loop using the kafka asynchronous commit api.
  Be aware that records from a partition must be processed and committed by a unique thread.
  If not, this could lead with non consistent behaviors. This is mostly useful with aggregation’s completion timeout strategies.

  If you want to use a custom implementation of KafkaManualCommit then you can configure a custom KafkaManualCommitFactory
  on the KafkaComponent that creates instances of your custom implementation.
   */
  private void doManualCommit(Exchange exchange) {
    Boolean lastOne = exchange.getIn()
        .getHeader(KafkaConstants.LAST_RECORD_BEFORE_COMMIT, Boolean.class);

    if (lastOne != null && lastOne) {
      KafkaManualCommit manual =
          exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
      if (manual != null) {
        LOGGER.info("manually committing the offset for batch");
        manual.commitSync();
        LOGGER.info("End time is {} ", LocalDateTime.now());
      }
    } else {
      LOGGER.info("NOT time to commit the offset yet");
    }
  }

  private void dumpKafkaDetails(Exchange exchange) {
    StringBuilder sb = new StringBuilder();
    sb.append("\r\n");
    sb.append("\r\n");
    sb.append("Message Received: ").append(exchange.getIn().getBody());
    sb.append("\r\n");
    sb.append("From topic: ")
        .append(exchange.getIn().getHeader(KafkaConstants.TOPIC));
    sb.append("\r\n");
    sb.append("From partition: ")
        .append(exchange.getIn().getHeader(KafkaConstants.PARTITION));
    sb.append("\r\n");
    sb.append("Offset: ").append(exchange.getIn().getHeader(KafkaConstants.OFFSET));
    sb.append("\r\n");
    sb.append("Is last record ?: ")
        .append(exchange.getIn().getHeader(KafkaConstants.LAST_RECORD_BEFORE_COMMIT));
    sb.append("\r\n");

    LOGGER.info(sb.toString());
  }

  private String buildKafkaUrl(String topicName) {
    StringBuilder sb = new StringBuilder("kafka:");
    sb.append(topicName)
        .append("?brokers=").append("localhost:9092")
        .append("&groupId=").append("kafkaConsumerGroup")
        .append("&maxPollRecords=").append(10) // Default is 500
        .append("&consumersCount=").append(1)
        .append("&autoOffsetReset=").append("earliest")
        .append("&autoCommitEnable=").append(false)
        .append("&allowManualCommit=").append(true)
        .append("&breakOnFirstError=").append(false);
    // commitTimeoutMs - https://camel.apache.org/components/3.15.x/kafka-component.html#_endpoint_query_option_commitTimeoutMs
    // heartbeatIntervalMs - https://camel.apache.org/components/3.15.x/kafka-component.html#_component_option_heartbeatIntervalMs
    // maxPollIntervalMs - https://camel.apache.org/components/3.15.x/kafka-component.html#_endpoint_query_option_maxPollIntervalMs
    // sessionTimeoutMs - https://camel.apache.org/components/3.15.x/kafka-component.html#_endpoint_query_option_sessionTimeoutMs

    return sb.toString();
  }

}
