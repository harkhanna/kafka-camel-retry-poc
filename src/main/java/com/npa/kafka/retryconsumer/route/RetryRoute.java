package com.npa.kafka.retryconsumer.route;

import java.io.IOException;
import java.time.LocalDateTime;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.Processor;
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

    restConfiguration().host("localhost").port("8000");

    String kafkaUrl = buildKafkaUrl();
    LOGGER.info("Kafka consumer URL is : {}", kafkaUrl);
    LOGGER.info("Start time is " + LocalDateTime.now().toString());

    onException(IOException.class)
        .log(LoggingLevel.WARN, "${exception.message}")
        .maximumRedeliveries(2)
        .redeliveryDelay(200)
        .useExponentialBackOff()
        .backOffMultiplier(2)
        .process(exchange -> {
          // Handle as per business requirement
          // Can send to another topic or DLQ
          doManualCommit(exchange);
        })
        // Introducing jitter is pending here
        .handled(true)
    ;

    from(kafkaUrl)
        .process(exchange -> {
          LOGGER.info(this.dumpKafkaDetails(exchange));
        })/*
        .process(exchange -> {
          String in = exchange.getIn().getBody(String.class);
          String out = in;
          out = new StringBuilder(in).reverse().toString();
          exchange.getIn().setBody(out);
        })
        .process(exchange -> {
          LOGGER.info("message is now> {}", exchange.getIn().getBody(String.class));
        })*/
        .log("before rest call")
        // Circuit breaker has been introduced after 3.xx of camel, but we are using 2.25.
        //.circuitBreaker()
        //.inheritErrorHandler(true) // Use defined error handler rather than Resilience CB handling
        //.resilience4jConfiguration().timeoutEnabled(true).timeoutDuration(2000).end()
        .to("rest:get:/customer/data")
        //.end()
        .log("end"); // This never gets logged as exception occurs in earlier line.
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
        LOGGER.info("End time is " + LocalDateTime.now().toString());
      }
    } else {
      LOGGER.info("NOT time to commit the offset yet");
    }
  }

  private String dumpKafkaDetails(Exchange exchange) {
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

    return sb.toString();
  }

  private String buildKafkaUrl() {
    StringBuilder sb = new StringBuilder();
    sb.append("kafka:")
        .append("retry_topic"); // TOPIC

    sb.append("?brokers=").append("localhost:9092")
    .append("&groupId=").append("kafkaConsumerGroup")
    .append("&maxPollRecords=").append(10) // TODO Find default
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
