package com.processor.processor.services;

import com.processor.processor.model.ParsedLog;
import org.apache.kafka.clients.producer.ProducerRecord;
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

    public void publishParsedLog(ParsedLog parsedLog, long collectorStartTime, String sourceFileName, String recordId, String batchId) {
        try {
            String json = objectMapper.writeValueAsString(parsedLog);

            ProducerRecord<String, String> record = new ProducerRecord<>("processor-topic", json);
            record.headers().add("processor_publish_time", String.valueOf(System.currentTimeMillis()).getBytes());
            record.headers().add("collector_start_time", String.valueOf(collectorStartTime).getBytes());
            record.headers().add("source_file_name", sourceFileName.getBytes());
            record.headers().add("record_id", recordId.getBytes());
            record.headers().add("batch_id", batchId.getBytes());

            kafkaTemplate.send(record);

        } catch (Exception e) {
            e.printStackTrace();

        }

    }
}
