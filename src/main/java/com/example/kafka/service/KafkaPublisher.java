package com.example.kafka.service;

import com.example.kafka.model.Books;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class KafkaPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPublisher.class);

    private KafkaTemplate<String, String> kafkaTemplate;
    private ObjectMapper obj;

    @Autowired
    public KafkaPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper obj) {
        this.kafkaTemplate = kafkaTemplate;
        this.obj = obj;
    }

    public void publishMessage(String message, String topicName) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                LOG.info("Sent message=[ {} ] with offset=[ {} ]",  message ,  result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                LOG.info("Unable to send message=[ {} ] due to : {}", message, ex.getMessage());
            }
        });
    }

    public void publishBooksUpdateMessage(Books books, String topicName) {
        StringBuilder jsonStr = new StringBuilder();
        try {
            jsonStr.append(obj.writeValueAsString(books));
            LOG.info("Rest API to publish a message to kafka topic {}", jsonStr);
        } catch (Exception ex) {
            LOG.error("Exception occurred", ex);
        }

        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, jsonStr.toString());
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                LOG.info("Sent message=[ {} ] with offset=[ {} ]",  jsonStr.toString() ,  result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(Throwable ex) {
                LOG.info("Unable to send message=[ {} ] due to : {}",  jsonStr.toString(), ex.getMessage());
            }
        });
    }
}
