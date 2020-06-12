package com.example.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class KafkaConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "book-update", groupId = "group_one")
    public void consume(String message) throws IOException {
        LOG.info(String.format("Consumed message -> %s", message));
    }
}
