package com.tapdata.tm.ws.cs;

import com.mongodb.client.model.changestream.FullDocument;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.bson.Document;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.MongoTemplate;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest;
import org.springframework.data.mongodb.core.messaging.DefaultMessageListenerContainer;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;
import static org.springframework.data.mongodb.core.query.Criteria.where;

@Configuration
public class MongoConfig {
    @Bean
    MessageListenerContainer messageListenerContainer(MongoTemplate template) {
        Executor executor = Executors.newSingleThreadExecutor();
        MessageListenerContainer messageListenerContainer = new DefaultMessageListenerContainer(template, executor) {
            @Override
            public boolean isAutoStartup() {
                return true;
            }
        };

        ChangeStreamRequest<Document> messageQueueRequest = buildMessageQueueRequest();
        messageListenerContainer.register(messageQueueRequest, Document.class);

        return messageListenerContainer;
    }

    private ChangeStreamRequest<Document> buildMessageQueueRequest(){
       return ChangeStreamRequest.builder(new MongoMessageListener())
                .collection("MessageQueue")
                .filter(newAggregation(match(where("operationType").in("insert"))))
                .fullDocumentLookup(FullDocument.UPDATE_LOOKUP)
                .build();
    }

    //@Bean
    MongoTransactionManager transactionManager(MongoDatabaseFactory factory){
        return new MongoTransactionManager(factory);
    }
}
