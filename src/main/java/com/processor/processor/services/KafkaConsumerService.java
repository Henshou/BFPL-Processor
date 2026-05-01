package com.processor.processor.services;

import com.processor.processor.model.ParsedLog;
import com.processor.processor.model.TelemetryEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;


@Service
public class KafkaConsumerService {
    private final LogParserService logParserService;
    private final KafkaPublisherService kafkaPublisherService;
    private final TelemetryPublisherService telemetryPublisherService;

    public KafkaConsumerService(LogParserService logParserService, KafkaPublisherService kafkaPublisherService, TelemetryPublisherService telemetryPublisherService) {
        this.logParserService = logParserService;
        this.kafkaPublisherService = kafkaPublisherService;
        this.telemetryPublisherService = telemetryPublisherService;
    }

    @KafkaListener(topics = "collector-topic", groupId = "processor-group")
    public void consumeFile(String message,
                            @Header("collector_start_time") byte[] startBytes,
                            @Header("collector_publish_time") byte[] publishBytes,
                            @Header("source_file_name") byte[] fileBytes,
                            @Header("record_id") byte[] recordIdBytes,
                            @Header("batch_id") byte[] batchIdBytes) {
        long startTime = Long.parseLong(new String(startBytes));
        long publishTime = Long.parseLong(new String(publishBytes));
        String fileName = new String(fileBytes);
        long receiveTime = System.currentTimeMillis();
        String recordId = new String(recordIdBytes);
        String batchId = new String(batchIdBytes);
        System.out.println("Received message: " + message);

        TelemetryEvent messagingDelayEvent = new TelemetryEvent(
                recordId,
                batchId,
                fileName,
                "processor",
                "messaging.delay.collector.processor",
                receiveTime - publishTime,
                System.currentTimeMillis()
        );

        telemetryPublisherService.publishTelemetry(messagingDelayEvent);

        long processingStartTime = System.currentTimeMillis();

        ParsedLog parsedLog = logParserService.parseRawLog(message);

        kafkaPublisherService.publishParsedLog(parsedLog, startTime, fileName, recordId, batchId);

        long processingEndTime = System.currentTimeMillis();

        TelemetryEvent processorEvent = new TelemetryEvent(
                recordId,
                batchId,
                fileName,
                "processor",
                "processor.record.process.time",
                processingEndTime - processingStartTime,
                System.currentTimeMillis()
        );

        telemetryPublisherService.publishTelemetry(processorEvent);

        System.out.println("Parsed Log:" + parsedLog);
    }
}
