package io.tapdata.proxy;

import io.tapdata.entity.annotations.Bean;
import io.tapdata.entity.annotations.Implementation;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.modules.api.net.message.MessageEntity;
import io.tapdata.modules.api.net.service.EventQueueService;
import io.tapdata.modules.api.net.service.MessageEntityService;
import io.tapdata.pdk.core.executor.ExecutorsManager;
import io.tapdata.pdk.core.utils.queue.SingleThreadBlockingQueue;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.List;

/**
 * Wrong way.
 * Can not use async queue to handle webhook messages, because when multiple servers, the sequence can hardly maintain between servers in async mode.
 * @deprecated
 */
@Implementation(value = EventQueueService.class, type = "async")
public class AsyncEventQueueService implements EventQueueService {
	private static final String TAG = AsyncEventQueueService.class.getSimpleName();
	@Bean
	private MessageEntityService messageEntityService;
	private SingleThreadBlockingQueue<MessageEntity> mongoBulkSavingQueue;

	private void handleBatchError(List<MessageEntity> messageEntities, Throwable throwable) {
		TapLogger.error(TAG, "handleBatchError count {} error {}", messageEntities != null ? messageEntities.size() : 0, ExceptionUtils.getStackTrace(throwable));
	}

	@Override
	public void offer(MessageEntity message) {
		if(mongoBulkSavingQueue == null) {
			synchronized (this) {
				if(mongoBulkSavingQueue == null) {
					mongoBulkSavingQueue = new SingleThreadBlockingQueue<MessageEntity>("MongoBulkSavingQueue")
							.withHandleSize(100)
							.withMaxSize(200)
							.withMaxWaitMilliSeconds(500)
							.withExecutorService(ExecutorsManager.getInstance().getExecutorService())
							.withHandler(this::handleBatch)
							.withErrorHandler(this::handleBatchError)
							.start();
				}
			}
		}
		mongoBulkSavingQueue.offer(message);
	}

	@Override
	public void newDataReceived(List<String> subscribeIds) {

	}

	private void handleBatch(List<MessageEntity> messageEntities) {
		messageEntityService.save(messageEntities, subscriptions -> {

		});
	}
}
