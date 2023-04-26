package io.tapdata.common;

import com.tapdata.constant.ConnectorContext;
import com.tapdata.constant.DateUtil;
import com.tapdata.entity.Event;
import com.tapdata.entity.Job;
import com.tapdata.entity.JobMessagePayload;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import io.tapdata.exception.SourceException;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

/**
 * @author samuel
 * @Description
 * @create 2021-04-16 15:50
 **/
public class BaseDDLHandler {

	protected ConnectorContext context;

	protected String DDL_EMAIL_TITLE = "Tapdata notification: DDL Warn, please perform DDL operation manually";

	public BaseDDLHandler(ConnectorContext context) {
		this.context = context;
	}

	/**
	 * 拼装EventData
	 *
	 * @return
	 */
	private Map<String, Object> getEventData(List<MessageEntity> messageEntities) {

		Job job = context.getJob();
		String jobName = job.getName();
		String source = context.getJobSourceConn().getName();
		String target = context.getJobTargetConn().getName();
		int i = 1;

		Map<String, Object> eventData = new HashMap<>();
		eventData.put("title", DDL_EMAIL_TITLE);

		StringBuilder sb = new StringBuilder("Job: ").append("<b>").append(jobName).append("</b>");
		sb.append("<br /><br />");
		sb.append("<b>Source: <font color=\"red\">").append(source).append("</b></font><br />");
		sb.append("<b>Target: <font color=\"red\">").append(target).append("</b></font><br />");
		sb.append("<b>Notification DDLs: <font color=\"black\"><br />");
		for (MessageEntity messageEntity : messageEntities) {
			sb.append("No. <font color=\"red\"><b>").append(i++).append("</b></font>&nbsp&nbsp&nbsp&nbsp");
			Long timestamp = DateUtil.convertTimestamp(messageEntity.getTimestamp(),
					TimeZone.getTimeZone(context.getJobSourceConn().getCustomZoneId()),
					TimeZone.getTimeZone("GMT"));
			sb.append("At: <font color=\"red\"><b>").append(DateUtil.timeStamp2Date(timestamp.toString(), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
					.append("</b></font><br />");
			sb.append("DDL Sql: <font color=\"red\"><b>").append(messageEntity.getDdl()).append("</b></font><br /><br />");
		}
		sb.append("<font color=\"red\"><b>").append("Please perform the corresponding ddl operation manually in the target database.").append("</b></font><br /><br />");
		sb.append(" </b></font>");
		eventData.put("message", sb.toString());

		return eventData;
	}

	protected MessageEntity wrapErrorMessage(List<MessageEntity> msgs) {
		Event event = new Event();
		event.setName(Event.EventName.DDL_WARN_EMAIL.name);
		event.setEvent_data(getEventData(msgs));
		event.setTag(Event.EVENT_TAG_USER);
		event.setJob_id(context.getJob().getId());
		event.setType(Event.EventName.DDL_WARN_EMAIL.name);
		String ddls = msgs.stream().filter(msg -> StringUtils.isNotBlank(msg.getDdl())).map(MessageEntity::getDdl).collect(Collectors.joining(","));
		final SourceException sourceException = new SourceException(String.format("There are some unhandled ddl(s). Please restart the job to confirm, ddl(s): %s",
				ddls), true);
		JobMessagePayload jobMessagePayload = new JobMessagePayload();
		jobMessagePayload.setJobErrorCause(sourceException);
		jobMessagePayload.setEmailEvent(event);
		final MessageEntity errorMessage = new MessageEntity();
		errorMessage.setJobMessagePayload(jobMessagePayload);
		errorMessage.setOp(OperationType.JOB_ERROR.getOp());
		return errorMessage;
	}
}
