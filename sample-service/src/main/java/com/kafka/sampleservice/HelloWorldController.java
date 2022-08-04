package com.kafka.sampleservice;

import java.time.LocalDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloWorldController {

  Logger log = LoggerFactory.getLogger(HelloWorldController.class);

  @GetMapping("/hello")
  public String printSomething(@RequestParam("message") String exchangeInfo) {

    log.info("Message received is " + exchangeInfo);
    return "Message received is " + exchangeInfo;
  }

}
