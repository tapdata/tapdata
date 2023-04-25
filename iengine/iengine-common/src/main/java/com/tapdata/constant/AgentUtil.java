package com.tapdata.constant;

import com.tapdata.entity.Worker;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class AgentUtil {

	private static final String AGENT_ID_YAML_PATH = "agent.yml";
	private static final String AGENT_ID_KEY = "agentId";

	public static String readAgentId(String workDir) {

		StringBuilder sb = new StringBuilder();
		if (StringUtils.isNotBlank(workDir)) {
			sb.append(workDir).append("/").append(AGENT_ID_YAML_PATH);
		} else {
			sb.append(AGENT_ID_YAML_PATH);
		}

		Yaml yaml = new Yaml();
		InputStream inputStream;
		try {
			inputStream = new FileInputStream(sb.toString());
		} catch (FileNotFoundException e) {
			return "";
		}
		if (inputStream == null) {
			return "";
		} else {
			Map<String, Object> data = yaml.load(inputStream);
			String agentId = data.get(AGENT_ID_KEY) + "";
			return agentId;
		}
	}

	public static String createAgentIdYaml(String workDir) throws IOException {
		StringBuilder sb = new StringBuilder();
		if (StringUtils.isNotBlank(workDir)) {
			sb.append(workDir).append("/").append(AGENT_ID_YAML_PATH);
		} else {
			sb.append(AGENT_ID_YAML_PATH);
		}
		String agentId = UUID.randomUUID().toString();
		Map<String, Object> data = new LinkedHashMap<>();
		data.put(AGENT_ID_KEY, agentId);
		Yaml yaml = new Yaml();
		FileWriter writer = new FileWriter(sb.toString());
		yaml.dump(data, writer);
		return agentId;
	}

	public static boolean isFirstWorker(ClientMongoOperator clientMongoOperator, String instanceNo, String userId, double workerTimeout) {
		if (clientMongoOperator != null && StringUtils.isNotBlank(instanceNo)) {
			// only first worker in charge of event(s)
			Criteria pingTime = where(Worker.PING_TIME).gte(Double.valueOf(System.currentTimeMillis() - workerTimeout * 1000));
			Criteria workType = where("worker_type").is(ConnectorConstant.WORKER_TYPE_TRANSFORMER);

			Criteria criteria = null;
			if (StringUtils.isNotBlank(userId)) {
				criteria = new Criteria().andOperator(
						pingTime,
						workType,
						where("user_id").is(userId)
				);
			} else {
				criteria = new Criteria().andOperator(pingTime, workType);
			}

			Query query = new Query(criteria).with(new Sort(Sort.Direction.ASC, Worker.PROCESS_ID));

			List<Worker> workers = clientMongoOperator.find(query, ConnectorConstant.WORKER_COLLECTION, Worker.class);

			if (CollectionUtils.isNotEmpty(workers)) {
				Worker firstWorker = workers.get(0);
				if (StringUtils.isNotBlank(firstWorker.getProcess_id())) {
					return firstWorker.getProcess_id().equals(instanceNo);
				} else {
					return false;
				}
			} else {
				return false;
			}
		}
		return false;
	}

	public static void main(String[] args) {
		String uuid = UUID.randomUUID().toString();
		System.out.println(uuid);
	}
}
