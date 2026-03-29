package com.processor.processor.services;

import com.processor.processor.model.ParsedLog;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    private final LogParserService logParserService;
    private final KafkaPublisherService kafkaPublisherService;

    public KafkaConsumerService(LogParserService logParserService, KafkaPublisherService kafkaPublisherService) {
        this.logParserService = logParserService;
        this.kafkaPublisherService = kafkaPublisherService;
    }

    @KafkaListener(topics = "collector-topic", groupId = "processor-group")
    public void consumeFile(String message) {
        System.out.println("Received message: " + message);

        ParsedLog parsedLog = logParserService.parseRawLog(message);

        kafkaPublisherService.publishParsedLog(parsedLog);

        System.out.println("Parsed Log:" + parsedLog);


    }
}
