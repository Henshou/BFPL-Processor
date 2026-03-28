package com.processor.processor.services;

import com.processor.processor.model.ParsedLog;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
public class KafkaPublisherService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaPublisherService(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper){
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void publishParsedLog(ParsedLog parsedLog) {
        try {
            String json = objectMapper.writeValueAsString(parsedLog);

            String topic = parsedLog.getOutput().getFirst();

            kafkaTemplate.send(topic, json);

            System.out.println("Sent parsed log to topic: " + topic);
        } catch (Exception e) {
            e.printStackTrace();

        }

    }
}
