package com.processor.processor.services;

import com.processor.processor.model.ParsedLog;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
public class LogParserService {
    public ParsedLog parseRawLog(String rawLog) {

        ParsedLog log = new ParsedLog();

        try{
            if (rawLog == null || rawLog.isEmpty()) {
                return null;
            }

            String[] parts = rawLog.split(";");
            if (parts.length < 3) {
                throw new RuntimeException("Invalid log format");
            }

            String dateTime = parts[0].trim();
            String username = parts[1].trim();

            String[] rest = parts[2].trim().split("\\s+");

            if (rest.length < 3) {
                throw new RuntimeException("Invalid log format");
            }

            String srcIp = rest[0].trim();
            String imei = rest[1].trim();
            String srcUser = rest[2].trim();

            String normalizedDateTime = normalizeDateTime(dateTime);

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS Z");

            OffsetDateTime odt = OffsetDateTime.parse(normalizedDateTime, formatter);
            long epochMillis = odt.toInstant().toEpochMilli();

            log.setDateTime(dateTime);
            log.setUsername(username);
            log.setSrcIp(srcIp);
            log.setImei(imei);
            log.setSrcUser(srcUser);
            log.setLogTime(epochMillis);
            log.setOutput(List.of("parsed_event"));

        } catch (Exception e) {
            log.setOutput(List.of("rawlog"));
        }

        return log;
    }

    private String normalizeDateTime(String dateTime) {
        String[] parts = dateTime.split(" ");

        String datePart = parts[0];
        String timePart = parts[1];
        String zonePart = parts[2];

        if (timePart.contains(".")) {
            String[] timeParts = timePart.split("\\.");
            String seconds = timeParts[0];
            String fractionPart = timeParts[1];

            // trim to 9 digits (nanoseconds)
            if (fractionPart.length() > 9) {
                fractionPart = fractionPart.substring(0, 9);
            }

            timePart = seconds + "." + fractionPart;
        }

        return datePart + " " + timePart + " " + zonePart;
    }
}
