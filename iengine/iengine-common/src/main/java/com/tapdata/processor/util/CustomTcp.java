package com.tapdata.processor.util;

import com.alibaba.fastjson.JSON;
import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import com.tapdata.constant.UUIDGenerator;
import com.tapdata.processor.dataflow.pb.DynamicProtoUtil;
import com.tapdata.processor.dataflow.pb.PbModel;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.voovan.network.exception.ReadMessageException;
import org.voovan.network.tcp.TcpSocket;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class CustomTcp {

	private static final Logger logger = LogManager.getLogger(CustomTcp.class);

	private static final TcpCacheMap tcpCacheMap = new TcpCacheMap();

	private static final Map<String, DynamicSchema> pbSchemaCacheMap = new ConcurrentHashMap<>();


	/**
	 * 组装报文头
	 *
	 * @param vinCode
	 * @param ts
	 * @param type    加密类型 eg. AES
	 * @param flag    命令标识
	 * @return
	 */
	public static byte[] assemble(String vinCode, long ts, String type, byte flag) {

		//构建Tcp消息的头部信息
		byte[] bytes = new byte[47];
		byte[] vinCodeBytes = vinCode.getBytes();
		bytes[0] = '#';
		bytes[1] = '#';
		//命令标识
		bytes[2] = flag;
		//vin
		System.arraycopy(vinCodeBytes, 0, bytes, 4, 17);
		//时间戳
		System.arraycopy(String.valueOf(ts).getBytes(), 0, bytes, 21, 13);
		//消息id
		String messageId = getStringRandom(9);
		byte[] messageIdBytes = messageId.getBytes();
		System.arraycopy(messageIdBytes, 0, bytes, 34, 9);

		return bytes;
	}

	public static byte[] pbConvert_util(String jsonStr, Message.Builder builder) {
		long l1 = System.nanoTime();
		try {
			JsonFormat.parser().merge(jsonStr, builder);
			byte[] pbMsgByteArray = builder.build().toByteArray();
			logger.debug("cost [{}]", System.nanoTime() - l1);
			return pbMsgByteArray;
		} catch (InvalidProtocolBufferException e) {
			logger.error("parse error", e);
			return new byte[0];
		}
	}

	/**
	 * @param dataMap
	 * @param schemaObj
	 * @param msgTypeName Unit
	 * @param pbSchemaKey
	 * @return
	 */
	public static byte[] pbConvert(Map<String, Object> dataMap, Object schemaObj, String msgTypeName, String pbSchemaKey) {
		String uuid = UUIDGenerator.uuid14(true);
		long l1 = System.nanoTime();
		if (logger.isDebugEnabled()) {
			logger.debug("[{}] pb convert pos{} nano time: [{}]", uuid, 1, l1);
		}
		String jsonStr = JSON.toJSONString(dataMap);
		if (logger.isDebugEnabled()) {
			long l2 = System.nanoTime();
			logger.debug("[{}] pb convert pos{} nano time: [{}]", uuid, 2, l2);
			logger.debug("[{}] pb convert step{} cost [{}]", uuid, 1, l2 - l1);
		}
		byte[] bytes = pbConvert(jsonStr, schemaObj, msgTypeName, pbSchemaKey, uuid);
		if (logger.isDebugEnabled()) {
			long l3 = System.nanoTime();
			logger.debug("[{}] pb convert pos{} nano time: [{}]", uuid, 99, l3);
			logger.debug("[{}] pb convert step{} cost [{}]", uuid, 99, l3 - l1);
		}
		return bytes;
	}

	public static byte[] pbConvert(Map<String, Object> dataMap, String schemaStr, String msgTypeName, String pbSchemaKey) {
		long l1 = System.nanoTime();
		String uuid = UUIDGenerator.uuid14(true);
		String jsonStr = JSON.toJSONString(dataMap);
		if (logger.isDebugEnabled()) {
			logger.debug("[{}] pb convert step{} cost [{}]", uuid, 1, System.nanoTime() - l1);
		}
		byte[] bytes = pbConvert(jsonStr, schemaStr, msgTypeName, pbSchemaKey, uuid);
		if (logger.isDebugEnabled()) {
			logger.debug("[{}] pb convert step{} cost [{}]", uuid, 9, System.nanoTime() - l1);
		}
		return bytes;
	}


	/**
	 * pb格式转换，并设置到map中
	 *
	 * @param pbSchemaKey
	 */
	public static byte[] pbConvert(String jsonStr, Object schemaObj, String msgTypeName, String pbSchemaKey, String uuid) {

		try {
			long l1 = System.nanoTime();
			if (logger.isDebugEnabled()) {
				logger.debug("[{}] pb convert pos{} nano time: [{}]", uuid, 3, l1);
			}
			DynamicSchema dynamicSchema = pbSchemaCacheMap.get(pbSchemaKey);
			if (dynamicSchema == null) {
				if (schemaObj instanceof ScriptObjectMirror) {
					schemaObj = toObject((ScriptObjectMirror) schemaObj);
				}
				if (logger.isDebugEnabled()) {
					logger.debug("[{}] pb convert pos{} nano time: [{}]", uuid, 4, System.nanoTime());
				}
				PbModel pbModel = JSON.parseObject(JSON.toJSONString(schemaObj), PbModel.class);
				if (logger.isDebugEnabled()) {
					logger.debug("[{}] pb convert pos{} nano time: [{}]", uuid, 5, System.nanoTime());
				}
				dynamicSchema = DynamicProtoUtil.generateSchema(pbModel);
				if (logger.isDebugEnabled()) {
					logger.debug("[{}] pb convert pos{} nano time: [{}]", uuid, 6, System.nanoTime());
				}
				pbSchemaCacheMap.put(pbSchemaKey, dynamicSchema);
				if (logger.isDebugEnabled()) {
					logger.debug("[{}] pb convert step{} cost [{}]", uuid, 2, System.nanoTime() - l1);
				}
			}
			if (logger.isDebugEnabled()) {
				long p2 = System.nanoTime();
				logger.debug("[{}] pb convert pos{} nano time: [{}]", uuid, 7, p2);
				logger.debug("[{}] pb convert step{} cost [{}]", uuid, 3, p2 - l1);
			}
			return pbConvert(jsonStr, dynamicSchema, msgTypeName, uuid);
		} catch (Descriptors.DescriptorValidationException e) {
			logger.error("protobuf convert error", e);
			return new byte[0];
		}
	}

	/**
	 * pb格式转换，并设置到map中
	 *
	 * @param pbSchemaKey
	 */
	public static byte[] pbConvert(String jsonStr, String pbSchemaStr, String msgTypeName, String pbSchemaKey, String uuid) {

		try {
			long l1 = System.nanoTime();

			DynamicSchema dynamicSchema = pbSchemaCacheMap.get(pbSchemaKey);
			if (dynamicSchema == null) {
				if (logger.isDebugEnabled()) {
					logger.debug("[{}] pbSchemaStr=[{}]", uuid, pbSchemaStr);
				}
				PbModel pbModel = JSON.parseObject(pbSchemaStr, PbModel.class);
				dynamicSchema = DynamicProtoUtil.generateSchema(pbModel);
				pbSchemaCacheMap.put(pbSchemaKey, dynamicSchema);
				if (logger.isDebugEnabled()) {
					logger.debug("[{}] pb convert step{} cost [{}]", uuid, 2, System.nanoTime() - l1);
				}
			}

			if (logger.isDebugEnabled()) {
				logger.debug("[{}] pb convert step{} cost [{}]", uuid, 3, System.nanoTime() - l1);
			}
			return pbConvert(jsonStr, dynamicSchema, msgTypeName, uuid);

		} catch (Descriptors.DescriptorValidationException e) {
			logger.error("protobuf convert error", e);
			return new byte[0];
		}
	}

	/**
	 * pb格式转换，并设置到map中
	 */
	public static byte[] pbConvert(String jsonStr, DynamicSchema dynamicSchema, String msgTypeName, String uuid) {

		try {
			long l1 = System.nanoTime();
			if (logger.isDebugEnabled()) {
				logger.debug("[{}] pb convert pos{} nano time: [{}]", uuid, 8, l1);
			}
			DynamicMessage.Builder builder = dynamicSchema.newMessageBuilder(msgTypeName);
			if (logger.isDebugEnabled()) {
				logger.debug("[{}] pb convert pos{} nano time: [{}]", uuid, 9, System.nanoTime());
			}
			JsonFormat.parser().merge(jsonStr, builder);
			if (logger.isDebugEnabled()) {
				logger.debug("[{}] pb convert pos{} nano time: [{}]", uuid, 10, System.nanoTime());
			}
			byte[] pbMsgByteArray = builder.build().toByteArray();
			if (logger.isDebugEnabled()) {
				long p2 = System.nanoTime();
				logger.debug("[{}] pb convert pos{} nano time: [{}]", uuid, 11, p2);
				logger.debug("[{}] pb convert step{} cost [{}]", uuid, 4, p2 - l1);
			}
			return pbMsgByteArray;
		} catch (InvalidProtocolBufferException e) {
			logger.error("protobuf convert error", e);
			return new byte[0];
		}
	}

	public static Object toObject(ScriptObjectMirror mirror) {
		if (mirror.isEmpty()) {
			return null;
		}
		if (mirror.isArray()) {
			List<Object> list = new ArrayList<>();
			for (Map.Entry<String, Object> entry : mirror.entrySet()) {
				Object result = entry.getValue();
				if (result instanceof ScriptObjectMirror) {
					list.add(toObject((ScriptObjectMirror) result));
				} else {
					list.add(result);
				}
			}
			return list;
		}

		Map<String, Object> map = new HashMap<>();
		for (Map.Entry<String, Object> entry : mirror.entrySet()) {
			Object result = entry.getValue();
			if (result instanceof ScriptObjectMirror) {
				map.put(entry.getKey(), toObject((ScriptObjectMirror) result));
			} else {
				map.put(entry.getKey(), result);
			}
		}
		return map;
	}

	/**
	 * @param host
	 * @param port
	 * @param readTimeout 读响应超时时间
	 * @param dataMap     数据map
	 * @param success     发送成功回调
	 * @param fail        发送失败回调
	 * @param noRsp       无响应回调
	 */
	public static void send(String host, int port, int readTimeout, Map<String, Object> dataMap,
							Consumer<Map<String, Object>> success,
							Consumer<Map<String, Object>> fail,
							Consumer<Map<String, Object>> noRsp) {
		try {
			TcpSocket tcpSocket = tcpCacheMap.get(host, port, readTimeout);
			byte[] byteData = bufBytes((byte[]) dataMap.get("head"), (byte[]) dataMap.get("unit"));
			tcpSocket.syncSend(ByteBuffer.wrap(byteData));
			byte[] response = null;
			try {
				Object syncRead = tcpSocket.syncRead();
				if (syncRead instanceof String) {
					response = ((String) syncRead).getBytes();
				}
			} catch (ReadMessageException e) {
				logger.error("read tcp response error {}", e.getMessage());
			}

			if (response == null) {
				noRsp.accept(dataMap);
			} else if (response[3] == 0x01) {
				success.accept(dataMap);
			} else if (response[3] == 0x02) {
				fail.accept(dataMap);
			} else {
				logger.error("response error", response);
			}

		} catch (IOException e) {
			logger.error("send tcp error", e);
		}
	}

	/**
	 * tcp数据发送
	 *
	 * @param host
	 * @param port
	 * @param dataStr
	 */
	public static void send(String host, int port, String dataStr) {
		try {
			TcpSocket tcpSocket = tcpCacheMap.get(host, port, 3000);
			byte[] byteData = dataStr.getBytes();
			tcpSocket.syncSend(ByteBuffer.wrap(byteData));
		} catch (IOException e) {
			logger.error("send tcp error", e);
		}
	}

	public static void login(String host, int port, String dataStr) {
		send(host, port, dataStr);
	}

	public static void logout(String host, int port, String dataStr) {
		send(host, port, dataStr);
	}

	public static void send_data(String host, int port, String dataStr) {
		send(host, port, dataStr);
	}

	/**
	 * 拼接byte数组
	 *
	 * @param begin 前数组
	 * @param after 后数组
	 * @return byte[]
	 * @author zhengwentao
	 * @date 2021/4/25 9:41
	 */
	public static byte[] bufBytes(byte[] begin, byte[] after) {
		byte[] result = new byte[begin.length + after.length];
		byte[] array = ByteBuffer.allocate(2).putShort((short) after.length).array();
		System.arraycopy(array, 0, begin, begin.length - 2, array.length);
		System.arraycopy(begin, 0, result, 0, begin.length);
		System.arraycopy(after, 0, result, begin.length, after.length);
		return result;
	}

	/**
	 * 随机生成包含大小写字母及数字的字符
	 *
	 * @param length
	 * @return java.lang.String
	 * @author lidi
	 * @date 2021/5/18 11:49
	 */
	private static String getStringRandom(int length) {
		String val = "";
		Random random = new Random();
		//参数length，表示生成几位随机数
		for (int i = 0; i < length; i++) {
			String charOrNum = random.nextInt(2) % 2 == 0 ? "char" : "num";
			//输出字母还是数字
			if ("char".equalsIgnoreCase(charOrNum)) {
				//输出是大写字母还是小写字母
				int temp = random.nextInt(2) % 2 == 0 ? 65 : 97;
				val += (char) (random.nextInt(26) + temp);
			} else if ("num".equalsIgnoreCase(charOrNum)) {
				val += String.valueOf(random.nextInt(10));
			}
		}
		return val;
	}
}
