package com.tapdata.tm.proxyprocess.service.impl;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.tapdata.tm.changestream.config.ChangeStreamManager;
import com.tapdata.tm.cluster.dto.RawServerStateDto;
import com.tapdata.tm.cluster.service.RawServerStateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.context.event.EventListener;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.messaging.Message;
import org.springframework.data.mongodb.core.messaging.MessageListener;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class OracleLogParserAgentProcessChangeListener
        implements MessageListener<ChangeStreamDocument<Document>, Document> {
    private static final String COLLECTION_NAME = "RawStateMetrix";

    private final MongoTemplate mongoTemplate;
    private final OracleLogParserAgentProcessProjectionService projectionService;
    private volatile MessageListenerContainer container;

    @EventListener(org.springframework.boot.context.event.ApplicationReadyEvent.class)
    public void start() {
        if (container != null && container.isRunning()) {
            return;
        }
        try {
            Aggregation filter = Aggregation.newAggregation(
                    Aggregation.match(org.springframework.data.mongodb.core.query.Criteria
                            .where("fullDocument.dataSource")
                            .in(RawServerStateService.ORACLE_LOG_PARSER_DATA_SOURCES))
            );
            container = ChangeStreamManager.start(
                    COLLECTION_NAME,
                    filter,
                    FullDocument.UPDATE_LOOKUP,
                    mongoTemplate,
                    this
            );
            log.info("Started OracleLogParser AgentProcess projection Change Stream");
        } catch (Exception e) {
            log.error("Failed to start OracleLogParser AgentProcess projection Change Stream", e);
        }
    }

    @Override
    public void onMessage(Message<ChangeStreamDocument<Document>, Document> message) {
        Document document = message.getBody();
        if (document == null) {
            return;
        }
        String dataSource = document.getString("dataSource");
        if (!OracleLogParserAgentProcessProjectionService.isOracleLogParserDataSource(dataSource)) {
            return;
        }

        String serviceId = document.getString("serviceId");
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                if (Boolean.TRUE.equals(document.getBoolean("delete"))) {
                    projectionService.refreshService(serviceId);
                } else {
                    RawServerStateDto state = mongoTemplate.getConverter().read(RawServerStateDto.class, document);
                    projectionService.handleReport(state);
                }
                return;
            } catch (Exception e) {
                if (attempt == 3) {
                    log.error("Failed to project OracleLogParser report for serviceId {} after {} attempts",
                            serviceId, attempt, e);
                    return;
                }
                log.warn("Retrying OracleLogParser projection for serviceId {} (attempt {}/3)",
                        serviceId, attempt + 1, e);
                try {
                    TimeUnit.MILLISECONDS.sleep(attempt * 100L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    log.warn("Interrupted while retrying OracleLogParser projection for serviceId {}", serviceId);
                    return;
                }
            }
        }
    }

    @PreDestroy
    public void stop() {
        ChangeStreamManager.stop(container);
    }
}
