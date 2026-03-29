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

            kafkaTemplate.send("processor-topic", json);

        } catch (Exception e) {
            e.printStackTrace();

        }

    }
}
