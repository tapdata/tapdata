package io.tapdata.dummy;

import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.Log4jUtil;
import com.tapdata.constant.MilestoneUtil;
import com.tapdata.constant.TestConnectionItemConstant;
import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.Job;
import com.tapdata.entity.Mapping;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.RelateDataBaseTable;
import com.tapdata.entity.Schema;
import com.tapdata.entity.Stats;
import io.tapdata.Source;
import io.tapdata.Target;
import io.tapdata.TargetExtend;
import io.tapdata.annotation.DatabaseTypeAnnotation;
import io.tapdata.common.JSONUtil;
import io.tapdata.common.SupportConstant;
import io.tapdata.entity.BaseConnectionValidateResult;
import io.tapdata.entity.BaseConnectionValidateResultDetail;
import io.tapdata.entity.ConnectionsType;
import io.tapdata.entity.LoadSchemaResult;
import io.tapdata.entity.OnData;
import io.tapdata.entity.SourceContext;
import io.tapdata.entity.TargetContext;
import io.tapdata.exception.SourceException;
import io.tapdata.exception.TargetException;
import io.tapdata.milestone.MilestoneStage;
import io.tapdata.milestone.MilestoneStatus;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@DatabaseTypeAnnotation(type = DatabaseTypeEnum.DUMMY)
public class DummySourceTarget implements Source, Target, TargetExtend {

	private long lastTimestamp = 0l;

	private List<BaseConnectionValidateResultDetail> validateResultDetails = new ArrayList<>();

	private SourceContext context;
	private TargetContext targetContext;

	private Map<String, Object> currOffset = new HashMap<>();

	private boolean stillRunning = true;

	private Integer increamentalInt = 1;

	private long lastChangeTimeStamp = 0l;

	private Logger logger;

	private int readSize = 10000;

	@Override
	public void sourceInit(SourceContext context) throws SourceException {
		this.context = context;
		this.logger = context.getLogger();

	}

	@Override
	public void initialSync() throws SourceException {
		Consumer<List<MessageEntity>> messageConsumer = context.getMessageConsumer();

		readSize = context.getSourceConn().getInitialReadSize();

		Job job = context.getJob();
		int readBatchSize = job.getReadBatchSize();
		List<Mapping> mappings = job.getMappings();

		List<MessageEntity> msgs = new ArrayList<>();

		job.setStatsTotalCount(context.getSourceConn(), this);

		for (Mapping mapping : mappings) {
			if (stillRunning) {
				String fromTable = mapping.getFrom_table();
				logger.info("Starting generate data in queue.Size: {}, table: {}", readSize, fromTable);
				for (int i = 1; i <= readSize; i++) {
					if (stillRunning) {
						msgs.add(buildMessageEntity(fromTable));

						if (i % readBatchSize == 0) {
							messageConsumer.accept(msgs);
							msgs.clear();
						}
					} else {
						logger.info("Stop initial sync.");
						return;
					}

				}

				if (CollectionUtils.isNotEmpty(msgs)) {
					messageConsumer.accept(msgs);
					msgs.clear();
				}

				logger.info("Finished initial sync.");
			} else {
				logger.info("Stop  initial sync.");
				return;
			}
		}
	}

	private MessageEntity buildMessageEntity(String fromTable) {
		MessageEntity msg = new MessageEntity();
		msg.setOp(ConnectorConstant.MESSAGE_OPERATION_INSERT);
		msg.setTableName(fromTable);

		Map<String, Object> after = new HashMap<>();
		msg.setAfter(after);
		after.put("id", increamentalInt);
		after.put("name", fromTable + increamentalInt);
		after.put("age", RandomUtils.nextInt(10, 100) + "");
		after.put("abc", RandomStringUtils.random(20));
		// delete this field since schema does not have this field
		// after.put("sym", "6F/Yk90ktCW1wfczFPASopzUpaXFvZG4ej9tuxEo9S/oY2DnyjX9U5x1S/GB34umGSP3Z2+2x4trr/gz6nVOiMeVzvmEq9axQkT2LWD0eqDHZbxUAt0ah73wneLPvKm3JXSvn7Ddah/OqFQ9EXtsso41gMYdIrRmuHBVKAupw0tGxU8gpp1vV0yJFqCa2lFPxoBLwYfaU3lTmHVm+Ompn3XCla9hVheg1/PGToaHSZMtailAq26b6Dq2dxGafOUinxalYovbike7xo8HfkzlpEiOW+8obRHDjOtfiry/CYO+RezY7VuozUueE3XSGgklO02bHVRdHnvYRCStNhYnS2RWbQ38rSEHQDITr5Q/k0apstQCyARbxagNUYnDXzMUC8HGeDcLT37HV3xU81TNQHktAQJo1WCIJWWtCRnvUz0ksS+5dIKQ5LDs3P8dL/RMbLCli+PTDfTGWOP0zv6oceaQB3A6eq/pQVqRdhe0SXW2+V3xeECrlOAzDyIydO3UEBGJeLGvkDkaMp34u7UJl8fcrA/1nHxDrqWAfMDaYkBHasbUEXWvHMHGgrheFqmN8O0EWEyroOvaSA5kO+kso9EnJhf52Aqy/7nXabY8Zslh32Ea4Qscw6yMhNPOGIxYUr2uiYzqFWrKphpk7rsVMNpJs8pYdPzL1+SWvpfSm4t2Krt5Xp5Byehr+bfHMqnybd5yWOH86W2AadMcQG95UXSSmD0BYkowYsh6hu/BQqCM8agGmR+ecY/yTgLydQadIB1R/5MA+FfS8XGbK3drtFofbA237W8GQB6WvDfrJf4U+okNmK6pAX9wbUnesx/DFwKrzDCm/AIIjGrT4dt4t7VfclZyUAAIV89zQBP75Pi4seoiYfGl3J6AdvORyYxxlu9cQePiIOh7BH0WrrvRxGOEx8yALXPCPxRtSZrICI+sBCFk+fYuou5c3Hsyb1SEiKxo43mISBYgY+Wzj6GIQVYjoHzbDt2Alr3AIy5i");

		Map<String, Object> offset = new HashMap<>();
		msg.setOffset(offset);
		offset.putAll(currOffset);
		offset.put(fromTable, after.get("id"));
		currOffset.putAll(offset);

		increamentalInt++;
		lastChangeTimeStamp = System.currentTimeMillis();
		msg.setTimestamp(lastChangeTimeStamp);

		return msg;
	}

	@Override
	public void increamentalSync() throws SourceException {

		int incrementalTps = context.getSourceConn().getIncreamentalTps();

		logger.info("Starting incremental sync, tps: {}.", incrementalTps);

		if (incrementalTps > 0) {
			// Milestone-READ_CDC_EVENT-FINISH
			MilestoneUtil.updateMilestone(context.getMilestoneService(), MilestoneStage.READ_CDC_EVENT, MilestoneStatus.FINISH);
			while (true) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					return;
				}

				if (stillRunning) {
					try {
						List<Mapping> mappings = context.getJob().getMappings();

						Mapping mapping = mappings.get(0);

						String fromTable = mapping.getFrom_table();

						Consumer<List<MessageEntity>> messageConsumer = context.getMessageConsumer();

						List<MessageEntity> msgs = new ArrayList<>();
						for (int i = 0; i < incrementalTps; i++) {
							if (stillRunning) {
								msgs.add(buildMessageEntity(fromTable));
							} else {
								logger.info("Stop {} incremental sync.", DatabaseTypeEnum.DUMMY);
								return;
							}
						}

						messageConsumer.accept(msgs);
						readSize += msgs.size();
					} catch (Exception e) {
						String errMsg = String.format("Read dummy cdc event failed, err: %s, stacks: %s", e.getMessage(), Log4jUtil.getStackString(e));

						MilestoneUtil.updateMilestone(context.getMilestoneService(), MilestoneStage.READ_CDC_EVENT, MilestoneStatus.ERROR, errMsg);
						if (!context.getJob().jobError(e, false, ConnectorConstant.SYNC_TYPE_CDC, logger, ConnectorConstant.WORKER_TYPE_CONNECTOR, errMsg, null)) {
							break;
						}
					}
				} else {
					logger.info("Stop {} incremental sync.", DatabaseTypeEnum.DUMMY);
					return;
				}
			}
		} else {
			logger.info("Stop {} incremental sync, because increamentalTps lte 0.", DatabaseTypeEnum.DUMMY);
		}
	}

	@Override
	public void sourceStop(Boolean force) throws SourceException {
		stillRunning = false;
	}

	@Override
	public int getSourceCount() throws SourceException {
		return readSize;
	}

	@Override
	public long getSourceLastChangeTimeStamp() throws SourceException {
		return lastChangeTimeStamp;
	}

	@Override
	public void targetInit(TargetContext context) throws TargetException {
		targetContext = context;
	}

	@Override
	public OnData onData(List<MessageEntity> msgs) throws TargetException {
		OnData onData = new OnData();
		onData.setOffset(msgs.get(msgs.size() - 1).getOffset());
		onData.setSource_received(msgs.size());
		onData.setProcessed(msgs.size());

		long inserted = 0L;
		long deleted = 0L;
		long updated = 0L;
		for (MessageEntity msg : msgs) {
			switch (msg.getOp()) {
				case ConnectorConstant.MESSAGE_OPERATION_INSERT:
					inserted++;
					break;
				case ConnectorConstant.MESSAGE_OPERATION_DELETE:
					deleted++;
					break;
				case ConnectorConstant.MESSAGE_OPERATION_UPDATE:
					updated++;
					break;
				default:
			}
			lastTimestamp = msg.getTimestamp() == null ? 0L : msg.getTimestamp();
			onData.incrementStatisticsStage(msg);
		}
		onData.increaseInserted(inserted);
		onData.increaseDeleted(deleted);
		onData.increaseUpdated(updated);

		return onData;
	}

	@Override
	public void targetStop(Boolean force) throws TargetException {

	}

	@Override
	public int getTargetCount() throws TargetException {
		Long targetInserted = 0l;
		Stats stats = targetContext.getJob().getStats();
		if (stats != null) {
			targetInserted = stats.getTotal().getOrDefault("target_inserted", 0l);
		}
		return targetInserted.intValue();
	}

	@Override
	public long getTargetLastChangeTimeStamp() throws TargetException {
		return lastTimestamp;
	}

	@Override
	public TargetContext getTargetContext() {
		return targetContext;
	}

	@Override
	public Map<String, Boolean> getSupported(String[] supports) {
		Map<String, Boolean> supportMap = new HashMap<>();
		for (String support : supports) {
			switch (support) {
				case SupportConstant.INITIAL_SYNC:
				case SupportConstant.ON_DATA:
				case SupportConstant.STATS:
				case SupportConstant.SYNC_PROGRESS:
					supportMap.put(support, true);
					break;
				default:
					supportMap.put(support, false);
					break;
			}
		}
		return supportMap;
	}

	@Override
	public List<BaseConnectionValidateResultDetail> connectionsInit(ConnectionsType connectionsType) {
		BaseConnectionValidateResultDetail validateResultDetail = new BaseConnectionValidateResultDetail(TestConnectionItemConstant.CHECK_CONFIG, true, "dummy1");
		validateResultDetails.add(validateResultDetail);
		return validateResultDetails;
	}

	@Override
	public BaseConnectionValidateResult testConnections(Connections connections) {
		for (BaseConnectionValidateResultDetail validateResultDetail : validateResultDetails) {
			validateResultDetail.setStatus(BaseConnectionValidateResultDetail.VALIDATE_DETAIL_RESULT_PASSED);
		}

		BaseConnectionValidateResult validateResult = new BaseConnectionValidateResult();
		validateResult.setValidateResultDetails(validateResultDetails);
		validateResult.setStatus(BaseConnectionValidateResult.CONNECTION_STATUS_READY);
		validateResult.setSchema(new Schema(loadSchema(connections).getSchema()));
		return validateResult;
	}

	@Override
	public LoadSchemaResult loadSchema(Connections connections) {
		LoadSchemaResult loadSchemaResult = new LoadSchemaResult();
		List<RelateDataBaseTable> relateDataBaseTables = null;
		try {
			relateDataBaseTables = JSONUtil.json2List("[{ " +
					"\"sym\" : \"6F/Yk90ktCW1wfczFPASopzUpaXFvZG4ej9tuxEo9S/oY2DnyjX9U5x1S/GB34umGSP3Z2+2x4trr/gz6nVOiMeVzvmEq9axQkT2LWD0eqDHZbxUAt0ah73wneLPvKm3JXSvn7Ddah/OqFQ9EXtsso41gMYdIrRmuHBVKAupw0tGxU8gpp1vV0yJFqCa2lFPxoBLwYfaU3lTmHVm+Ompn3XCla9hVheg1/PGToaHSZMtailAq26b6Dq2dxGafOUinxalYovbike7xo8HfkzlpEiOW+8obRHDjOtfiry/CYO+RezY7VuozUueE3XSGgklO02bHVRdHnvYRCStNhYnS2RWbQ38rSEHQDITr5Q/k0apstQCyARbxagNUYnDXzMUC8HGeDcLT37HV3xU81TNQHktAQJo1WCIJWWtCRnvUz0ksS+5dIKQ5LDs3P8dL/RMbLCli+PTDfTGWOP0zv6oceaQB3A6eq/pQVqRdhe0SXW2+V3xeECrlOAzDyIydO3UEBGJeLGvkDkaMp34u7UJl8fcrA/1nHxDrqWAfMDaYkBHasbUEXWvHMHGgrheFqmN8O0EWEyroOvaSA5kO+kso9EnJhf52Aqy/7nXabY8Zslh32Ea4Qscw6yMhNPOGIxYUr2uiYzqFWrKphpk7rsVMNpJs8pYdPzL1+SWvpfSm4t2Krt5Xp5Byehr+bfHMqnybd5yWOH86W2AadMcQG95UXSSmD0BYkowYsh6hu/BQqCM8agGmR+ecY/yTgLydQadIB1R/5MA+FfS8XGbK3drtFofbA237W8GQB6WvDfrJf4U+okNmK6pAX9wbUnesx/DFwKrzDCm/AIIjGrT4dt4t7VfclZyUAAIV89zQBP75Pi4seoiYfGl3J6AdvORyYxxlu9cQePiIOh7BH0WrrvRxGOEx8yALXPCPxRtSZrICI+sBCFk+fYuou5c3Hsyb1SEiKxo43mISBYgY+Wzj6GIQVYjoHzbDt2Alr3AIy5i\", " +
					"\"table_name\" : \"test\", \"fields\" : [ { \"field_name\" : \"id\", \"table_name\" : \"test\", \"dataType\" : 4, \"javaType\" : \"Integer\", \"data_type\" : \"integer\", \"primary_key_position\" : 1, \"foreign_key_table\" : null, \"foreign_key_column\" : null, \"key\" : null, \"precision\" : null, \"scale\" : 10 }, { \"field_name\" : \"name\", \"table_name\" : \"test\", \"data_type\" : \"string\", \"dataType\" : 12, \"javaType\" : \"String\", \"primary_key_position\" : 0, \"foreign_key_table\" : null, \"foreign_key_column\" : null, \"key\" : null, \"precision\" : null, \"scale\" : null }, { \"field_name\" : \"age\", \"table_name\" : \"test\", \"data_type\" : \"string\", \"dataType\" : 12, \"javaType\" : \"String\", \"primary_key_position\" : 0, \"foreign_key_table\" : null, \"foreign_key_column\" : null, \"key\" : null, \"precision\" : null, \"scale\" : null }, { \"field_name\" : \"abc\", \"table_name\" : \"test\", \"data_type\" : \"string\", \"dataType\" : 12, \"javaType\" : \"String\", \"primary_key_position\" : 0, \"foreign_key_table\" : null, \"foreign_key_column\" : null, \"key\" : null, \"precision\" : null, \"scale\" : null } ], \"cdc_enabled\" : null }]", RelateDataBaseTable.class);
		} catch (IOException e) {
			logger.error("Load connection {} schema failed {}", connections.getName(), e.getMessage());
		}
		loadSchemaResult.setSchema(relateDataBaseTables);
		return loadSchemaResult;
	}

	@Override
	public Long count(String objectName, Connections connections) {
		return (long) readSize;
	}

	@Override
	public Long count(String objectName, Connections connections, Mapping mapping) {
		return (long) readSize;
	}
}
