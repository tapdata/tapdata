package io.tapdata.Schedule;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.entity.Event;
import com.tapdata.entity.TapLog;
import com.tapdata.entity.User;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.WarningMaker;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Query;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class JobListener {

	private Logger logger = LogManager.getLogger(getClass());

	private WarningMaker warningMaker;

	private ClientMongoOperator clientMongoOperator;

	public JobListener(WarningMaker warningMaker, ClientMongoOperator clientMongoOperator) {
		this.warningMaker = warningMaker;
		this.clientMongoOperator = clientMongoOperator;
	}

	public void listen() {

		List<Bson> pipeline = Collections.singletonList(Aggregates.match(
				Filters.and(
						Filters.ne("operationType", "delete"),
						Filters.ne("operationType", "invalidate")
				)
		));

		MongoCollection<Document> collection = clientMongoOperator.getMongoTemplate().getCollection(ConnectorConstant.JOB_COLLECTION);
		MongoCursor<ChangeStreamDocument<Document>> cursor = collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP).iterator();

		boolean error = false;
		while (true) {
			try {
				if (error) {
					cursor = collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP).iterator();
					error = false;
				}
				ChangeStreamDocument<Document> csDoc = cursor.tryNext();
				if (csDoc != null) {
					warnEmail(csDoc);
				} else {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {

					}
				}

			} catch (Exception e) {
				logger.error(TapLog.ERROR_0003.getMsg(), e.getMessage(), e);

				try {
					Thread.sleep(3000);
				} catch (InterruptedException e1) {

				}
				error = true;

			}
		}
	}

	private void warnEmail(ChangeStreamDocument<Document> csDoc) {
		OperationType operationType = csDoc.getOperationType();

		if (operationType != OperationType.UPDATE && operationType != OperationType.REPLACE) {
			return;
		}
		Map<String, Object> eventData = new HashMap<>();
		Document fullDocument = csDoc.getFullDocument();
		if (fullDocument != null) {
			String jobName = fullDocument.getString("name");
			String userId = fullDocument.getString("user_id");

			Boolean eventJobError = fullDocument.getBoolean("event_job_error");
			Boolean eventJobStarted = fullDocument.getBoolean("event_job_started");
			Boolean eventJobStopped = fullDocument.getBoolean("event_job_stopped");
			BsonString status = null;
			if (csDoc.getUpdateDescription().getUpdatedFields().containsKey("status")) {
				status = csDoc.getUpdateDescription().getUpdatedFields().getString("status");
			}

			if (status != null && !status.equals("scheduled") && needToSendMail(eventJobError, eventJobStarted, eventJobStopped, status)) {
				List<User> users = clientMongoOperator.find(new Query(where("_id").is(userId)), "users", User.class);
				if (CollectionUtils.isNotEmpty(users)) {
					User user = users.get(0);
					String email = user.getEmail();

					StringBuilder sb = new StringBuilder("Job ").append(jobName).append(" was modified");
					sb.append("<br /<br />");
					sb.append("Status:<font color=\"red\"><b>").append(status.getValue()).append("</b></font>");

					eventData.put("title", "Tapdata Notification: Job " + status.getValue().substring(0, 1).toUpperCase() + status.getValue().substring(1));
					eventData.put("message", sb.toString());
					eventData.put("trigger", WarningEvent.JOB_EDITTED.warningEvent);
					eventData.put("context_data", csDoc.getUpdateDescription().getUpdatedFields());
					eventData.put("receiver", email);

					BsonDocument documentKey = csDoc.getDocumentKey();
					ObjectId jobId = documentKey.getObjectId("_id").getValue();
					warningMaker.generateEvent(Event.EventName.WARN_EMAIL, eventData, Event.EVENT_TAG_USER, jobId.toHexString());
				}
			}
		}
	}

	private boolean needToSendMail(Boolean eventJobError, Boolean eventJobStarted, Boolean eventJobStopped, BsonString status) {
		return (eventJobStarted != null && eventJobStarted && ConnectorConstant.RUNNING.equals(status.getValue())) ||
				(eventJobError != null && eventJobError && ConnectorConstant.ERROR.equals(status.getValue())) ||
				(eventJobStopped != null && eventJobStopped && ConnectorConstant.PAUSED.equals(status.getValue()));

	}

	public enum WarningEvent {
		JOB_STOPPED("job-stopped"),
		JOB_EDITTED("job-editted"),
		JOB_ERROR("job-error"),
		JOB_STARTED("job-started"),
		DATABASE_OPERTION("database-operation");

		public String warningEvent;

		WarningEvent(String warningEvent) {
			this.warningEvent = warningEvent;
		}

		public String getWarningEvent() {
			return warningEvent;
		}
	}

}
