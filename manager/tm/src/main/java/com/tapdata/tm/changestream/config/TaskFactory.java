package com.tapdata.tm.changestream.config;

import java.util.function.Supplier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest;
import org.springframework.data.mongodb.core.messaging.SubscriptionRequest;
import org.springframework.data.mongodb.core.messaging.TailableCursorRequest;
import org.springframework.data.mongodb.core.messaging.Task;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;

class TaskFactory {

	private final MongoTemplate tempate;

	/**
	 * @param template must not be {@literal null}.
	 */
	TaskFactory(MongoTemplate template) {

		Assert.notNull(template, "Template must not be null!");

		this.tempate = template;
	}

	/**
	 * Create a {@link Task} for the given {@link SubscriptionRequest}.
	 *
	 * @param request must not be {@literal null}.
	 * @param targetType must not be {@literal null}.
	 * @param errorHandler must not be {@literal null}.
	 * @return must not be {@literal null}. Consider {@code Object.class}.
	 * @throws IllegalArgumentException in case the {@link SubscriptionRequest} is unknown.
	 */
	<S, T> Task forRequest(SubscriptionRequest<S, ? super T, ? extends SubscriptionRequest.RequestOptions> request, Class<T> targetType,
	                       ErrorHandler errorHandler, Supplier<Boolean> containerRunning) {

		Assert.notNull(request, "Request must not be null!");
		Assert.notNull(targetType, "TargetType must not be null!");

		if (request instanceof ChangeStreamRequest) {
			return new ChangeStreamTask(tempate, (ChangeStreamRequest) request, targetType, errorHandler, containerRunning);
		} else if (request instanceof TailableCursorRequest) {
			return new TailableCursorTask(tempate, (TailableCursorRequest) request, targetType, errorHandler);
		}

		throw new IllegalArgumentException(
				"oh wow - seems you're using some fancy new feature we do not support. Please be so kind and leave us a note in the issue tracker so we can get this fixed.\nThank you!");
	}
}