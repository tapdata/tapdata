/**
 * @title: ChangeStreamManager
 * @description:
 * @author lk
 * @date 2021/9/11
 */
package com.tapdata.tm.changestream.config;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest;
import org.springframework.data.mongodb.core.messaging.DefaultMessageListenerContainer;
import org.springframework.data.mongodb.core.messaging.MessageListener;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;

public class ChangeStreamManager {

	private static final ThreadPoolExecutor changeStreamThreadPool = new ThreadPoolExecutor(5, 5,
				0,TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

	public static MessageListenerContainer start(String collectionName, Aggregation aggregation, FullDocument lookup, MongoTemplate mongoTemplate, MessageListener<ChangeStreamDocument<Document>, Document> listener){
		MessageListenerContainer container = new MyMessageListenerContainer(mongoTemplate, changeStreamThreadPool);
		container.start();

		ChangeStreamRequest.ChangeStreamRequestBuilder<Document> builder = ChangeStreamRequest.builder(listener)
				.collection(collectionName);
		if (aggregation != null) {
			builder.filter(aggregation);
		}
		if (lookup != null) {
			builder.fullDocumentLookup(lookup);
		}
		container.register(builder.build(), Document.class);

		return container;
	}

	public static <T> MessageListenerContainer start(String collectionName, Aggregation aggregation, FullDocument lookup, MongoTemplate mongoTemplate, MessageListener<ChangeStreamDocument<Document>, T> listener, Class<T> tClass){
		MessageListenerContainer container = new MyMessageListenerContainer(mongoTemplate,changeStreamThreadPool);
		container.start();

		ChangeStreamRequest.ChangeStreamRequestBuilder<T> builder = ChangeStreamRequest.builder(listener)
				.collection(collectionName);
		if (aggregation != null) {
			builder.filter(aggregation);
		}
		if (lookup != null) {
			builder.fullDocumentLookup(lookup);
		}
		container.register(builder.build(), tClass);

		return container;
	}

	public static void stop(MessageListenerContainer container){
		if (container != null && container.isRunning()){
			container.stop();
		}
	}
}
