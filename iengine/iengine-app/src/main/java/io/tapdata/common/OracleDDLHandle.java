package io.tapdata.common;

import com.tapdata.constant.ConnectorContext;
import com.tapdata.entity.MessageEntity;
import com.tapdata.entity.OperationType;
import com.tapdata.validator.SchemaFactory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class OracleDDLHandle extends BaseDDLHandler implements DdlHandler {

	private static final Logger logger = LogManager.getLogger(OracleDDLHandle.class);


	public OracleDDLHandle(ConnectorContext context) {
		super(context);
	}

	@Override
	public List<MessageEntity> handleMessage(List<MessageEntity> messageEntities) {
		//todo oracle在源端实现了过滤的逻辑，需要统一移植到这里

		//刷新源端模型
		Set<String> tableSet = new LinkedHashSet<>();
		if (CollectionUtils.isNotEmpty(messageEntities)) {
			for (MessageEntity messageEntity : messageEntities) {
				if (OperationType.DDL.getOp().equalsIgnoreCase(messageEntity.getOp())) {
					tableSet.add(messageEntity.getTableName());
				}
			}
		}
		if (CollectionUtils.isNotEmpty(tableSet)) {
			try {
				logger.info("Found the DDLs, Update source schemas: {}", String.join(",", tableSet));
				for (String tableName : tableSet) {
					SchemaFactory.updateSchema(context.getJob().getClientMongoOperator(), context.getJobSourceConn(), tableName);
				}
			} catch (Exception e) {
				throw new RuntimeException("Refresh source schema failed.", e);
			}
		}

		return messageEntities;
	}
}
