package com.kafka.retryconsumer.route;


import com.kafka.retryconsumer.client.PocRestClient;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaManualCommit;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@AllArgsConstructor
@Component
public class RetryRoute extends RouteBuilder {

    private static final String KAFKA_ENDPOINT = "kafka:%s?groupId=%s";
    private final PocRestClient pocRestClient;
    private final RetryTemplate retryInstanceTopic1;

    @Override
    public void configure() {

        fromF(KAFKA_ENDPOINT, "retry_topic_1", "group_id_1")
                .routeId("R1")
                .process(exchange -> log.info(this.dumpKafkaDetails(exchange)))
                .log("before rest call 1")
                .doTry()
                .process(exchange -> retryInstanceTopic1.execute(context -> {
                    pocRestClient.restClient1(exchange);
                    return null;
                }))
                .doCatch(Exception.class)
                .log("error topic producer goes here...")
                .end()
                .process(this::doManualCommit)
                .log("end");

        fromF(KAFKA_ENDPOINT, "retry_topic_2", "group_id_2")
                .routeId("R2")
                .process(exchange -> log.info(this.dumpKafkaDetails(exchange)))
                .log("before rest call 2")
                .doTry()
                .process(pocRestClient::restClient2)
                .doCatch(Exception.class)
                .log("error topic producer goes here...")
                .end()
                .process(this::doManualCommit)
                .log("end");
    }

    private void doManualCommit(Exchange exchange) {
        Boolean lastOne = exchange.getIn()
                .getHeader(KafkaConstants.LAST_RECORD_BEFORE_COMMIT, Boolean.class);

        if (lastOne != null && lastOne) {
            KafkaManualCommit manual =
                    exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
            if (manual != null) {
                log.info("manually committing the offset for batch");
                manual.commitSync();
                log.info("End time is {} ", LocalDateTime.now());
            }
        } else {
            log.info("NOT time to commit the offset yet");
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
}
