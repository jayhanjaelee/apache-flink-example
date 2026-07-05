package com.example.flinkdemo.parser;

import com.example.flinkdemo.model.UserEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserEventParser implements FlatMapFunction<String, UserEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(UserEventParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void flatMap(String rawJson, Collector<UserEvent> out) {
        if (rawJson == null || rawJson.isBlank()) {
            return;
        }
        try {
            UserEvent event = MAPPER.readValue(rawJson, UserEvent.class);
            if (event.userId == null || event.action == null) {
                LOG.warn("Skipping event missing required fields: {}", rawJson);
                return;
            }
            out.collect(event);
        } catch (Exception e) {
            LOG.warn("Skipping unparsable message: {} ({})", rawJson, e.toString());
        }
    }
}
