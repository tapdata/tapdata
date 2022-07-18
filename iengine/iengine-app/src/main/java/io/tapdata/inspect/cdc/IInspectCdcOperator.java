package io.tapdata.inspect.cdc;

import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.inspect.InspectCdcWinData;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.inspect.cdc.exception.InspectCdcNonsupportException;
import io.tapdata.inspect.cdc.operator.KafkaInspectCdcOperator;
import io.tapdata.inspect.cdc.operator.ShareLogInspectCdcOperator;

import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 增量源操作接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/3 上午11:38 Create
 */
public interface IInspectCdcOperator extends AutoCloseable {

	// 是否源操作
	boolean isSource();

	/**
	 * 将增量窗口事件数结果转为Map
	 *
	 * @param cdcWinData 增量窗口配置
	 * @param counts     增量数据量
	 * @return 增量窗口事件数结果
	 */
	default Map<String, Object> cdcWinData2Map(InspectCdcWinData cdcWinData, long counts) {
		Map<String, Object> map = new LinkedHashMap<>();
		map.put("winBegin", InspectCdcUtils.format(cdcWinData.getWinBegin()));
		map.put("winEnd", InspectCdcUtils.format(cdcWinData.getWinEnd()));
		map.put("dataBegin", InspectCdcUtils.format(cdcWinData.getDataBegin()));
		map.put("dataEnd", InspectCdcUtils.format(cdcWinData.getDataEnd()));
		map.put("beginOffset", cdcWinData.getBeginOffset());
		map.put("endOffset", cdcWinData.getEndOffset());
		map.put("counts", counts);
		return map;
	}

	/**
	 * 统计窗口事件量
	 *
	 * @param cdcWinData 增量窗口数据
	 * @return 窗口事件量
	 */
	long count(InspectCdcWinData cdcWinData);

	/**
	 * 获取最后一条事件操作时间
	 *
	 * @return 时间
	 */
	Instant lastEventDate();

	/**
	 * 生成增量源操作实例
	 *
	 * @param connections         连接
	 * @param taskId              任务编号
	 * @param clientMongoOperator 中间库操作实例
	 * @param isSource            是否为源
	 * @return 增量源操作实例
	 */
	static IInspectCdcOperator build(ClientMongoOperator clientMongoOperator, String taskId, Connections connections, String tableName, boolean isSource) throws UnsupportedEncodingException {
		DatabaseTypeEnum databaseTypeEnum = DatabaseTypeEnum.fromString(connections.getDatabase_type());
		String errMsg = String.format("Nonsupport CDC operator %s type is '%s'", isSource ? "source" : "target", databaseTypeEnum);
		if (null != databaseTypeEnum) {
			if (isSource) {
				return ShareLogInspectCdcOperator.buildBySourceConnection(clientMongoOperator, connections, tableName); // 共享日志
			} else if (DatabaseTypeEnum.KAFKA == databaseTypeEnum) {
				return new KafkaInspectCdcOperator(false, taskId, connections, tableName); // Kafka队列
			}
		}
		throw new InspectCdcNonsupportException(errMsg);
	}
}
