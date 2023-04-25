package io.tapdata.cdc.event;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.ExecutorUtil;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MapUtil;
import com.tapdata.entity.CdcEvent;
import com.tapdata.entity.Connections;
import com.tapdata.entity.Job;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.SyncStageEnum;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.NamedThreadFactory;
import org.bson.Document;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author samuel
 * @Description
 * @create 2020-09-03 19:12
 **/
public class CdcEventHandler implements Serializable {

	private static final long serialVersionUID = 6226267268477650829L;
	private static final int BATCH_SIZE = 500;
	private static final String CDC_EVENT_HANDLER_THREAD_NAME = "CDC-EVENT-HANDLER";

	private Logger logger = LogManager.getLogger(CdcEventHandler.class);
	private Job job;
	private Connections sourceConn;
	private Connections targetConn;
	private ClientMongoOperator clientMongoOperator;
	private ExecutorService handleCdcEventsThreadPool;
	private LinkedBlockingQueue<CdcEvent> cdcEventLinkedBlockingQueue;
	private List<CdcEvent> cdcEventsTemp;
	private Lock lock;

	public CdcEventHandler(Job job, Connections sourceConn, Connections targetConn, ClientMongoOperator clientMongoOperator) {
		this.job = job;
		this.sourceConn = sourceConn;
		this.targetConn = targetConn;
		this.clientMongoOperator = clientMongoOperator;
		this.cdcEventLinkedBlockingQueue = new LinkedBlockingQueue<>();
		this.cdcEventsTemp = new LinkedList<>();
		lock = new ReentrantLock();
		initCdcEventHandler();
	}

	private void initCdcEventHandler() {
		handleCdcEventsThreadPool = new ThreadPoolExecutor(
				0, 1, 0L, TimeUnit.SECONDS,
				new SynchronousQueue<>(), new NamedThreadFactory(CDC_EVENT_HANDLER_THREAD_NAME)
		);

		handleCdcEventsThreadPool.submit(() -> {
			try {
				while (job.isRunning()) {
					Log4jUtil.setThreadContext(job);
					CdcEvent cdcEvent = cdcEventLinkedBlockingQueue.poll();
					if (cdcEvent == null) {
						try {
							Thread.sleep(500L);
							continue;
						} catch (InterruptedException e) {
							break;
						}
					}

					if (cdcEvent.isBatchLast()) {
						insertCdcEvents();
						continue;
					}
					cdcEventsTemp.add(cdcEvent);

					if (cdcEventsTemp.size() % BATCH_SIZE == 0) {
						insertCdcEvents();
					}
				}
				if (CollectionUtils.isNotEmpty(cdcEventsTemp)) {
					insertCdcEvents();
				}
			} catch (IllegalAccessException e) {
				logger.error("Automatically save cdc events error, err msg: {}, job name: {}, source conn name: {}",
						e.getMessage(), job.getName(), sourceConn.getName(), e);
			}
		});
	}

	public void saveCdcEvents(List<MessageEntity> messageEntityList) {
		if (CollectionUtils.isEmpty(messageEntityList)) {
			return;
		}

		if (!validateVariables()) {
			return;
		}

		Iterator<MessageEntity> iterator = messageEntityList.iterator();
		boolean hasValidMessage = false;
		while (iterator.hasNext() && job.isRunning()) {
			MessageEntity messageEntity = iterator.next();

			if (!validateMessageEntity(messageEntity)) {
				continue;
			}

			CdcEvent cdcEvent = null;
			try {
				cdcEvent = new CdcEvent(messageEntity, sourceConn);
			} catch (Exception e) {
				throw new RuntimeException(String.format("Clone message entity %s to cdc event failed %s", messageEntity, e.getCause()), e);
			}

			try {
				cdcEventLinkedBlockingQueue.put(cdcEvent);
			} catch (InterruptedException e) {
				break;
			}
			hasValidMessage = true;
		}
		if (hasValidMessage) {
			try {
				cdcEventLinkedBlockingQueue.put(new CdcEvent(true));
			} catch (InterruptedException e) {
				return;
			}
		}
	}

	private boolean validateVariables() {
		return job != null && sourceConn != null && targetConn != null && clientMongoOperator != null;
	}

	private boolean validateMessageEntity(MessageEntity messageEntity) {
		return messageEntity.getSyncStage().equals(SyncStageEnum.CDC)
				&& StringUtils.equalsAny(messageEntity.getOp(),
				ConnectorConstant.MESSAGE_OPERATION_INSERT,
				ConnectorConstant.MESSAGE_OPERATION_UPDATE,
				ConnectorConstant.MESSAGE_OPERATION_DELETE);
	}

	public void stop(boolean force) {
		if (force) {
			Optional.ofNullable(handleCdcEventsThreadPool).ifPresent(tp -> tp.shutdownNow());
		} else {
			ExecutorUtil.shutdown(handleCdcEventsThreadPool, 5L, TimeUnit.SECONDS);
		}
	}

	private void insertCdcEvents() throws IllegalAccessException {
		if (CollectionUtils.isEmpty(cdcEventsTemp)) {
			return;
		}
		List<WriteModel<Document>> writeModels = new LinkedList<>();
		Iterator<CdcEvent> iterator = cdcEventsTemp.iterator();
		while (iterator.hasNext() && job.isRunning()) {
			CdcEvent cdcEvent = iterator.next();
			Document document = MapUtil.obj2Document(cdcEvent);
			document.remove("serialVersionUID");
			document.remove("batchLast");
			Object source = document.getOrDefault("source", null);
			document.remove("source");
			if (source != null) {
				document.put("source", MapUtil.obj2Document(source));
				((Document) document.get("source")).remove("this$0");
			}
			writeModels.add(new InsertOneModel(document));
		}

		try {
			MongoCollection<Document> collection = clientMongoOperator.getMongoTemplate().getCollection(ConnectorConstant.CDC_EVENTS_COLLECTION);
			BulkWriteResult bulkWriteResult = collection.bulkWrite(writeModels);
			if (logger.isDebugEnabled()) {
				logger.debug("Save cdc event succeed, bulk write result: {}", bulkWriteResult.getInsertedCount());
			}
		} catch (Exception e) {
			logger.warn("Save cdc events error, err msg: {}, stacks: {}", e.getMessage(), Log4jUtil.getStackString(e));
		} finally {
			cdcEventsTemp.clear();
		}
	}
}
