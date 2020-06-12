package com.example.kafka.resources;

import com.example.kafka.model.Books;
import com.example.kafka.service.KafkaPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/kafka")
public class KafkaResource {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaResource.class);

    private KafkaPublisher kafkaPublisher;

    @Autowired
    public KafkaResource(KafkaPublisher kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
    }

    @RequestMapping(value = "publish", method = RequestMethod.POST)
    public ResponseEntity<String> publishMessage(@RequestBody String message, @RequestHeader(name = "topic-name") String topicName) {
        kafkaPublisher.publishMessage(message, topicName);
        return ResponseEntity.ok().build();
    }

    @RequestMapping(value = "books/publish", method = RequestMethod.POST)
    public ResponseEntity<String> publishMessage(@RequestBody Books books, @RequestHeader(name = "topic-name") String topicName) {
        kafkaPublisher.publishBooksUpdateMessage(books, topicName);
        return ResponseEntity.ok().build();
    }

}
