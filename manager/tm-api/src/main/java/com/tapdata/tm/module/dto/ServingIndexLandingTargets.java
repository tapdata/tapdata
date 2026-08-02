package com.tapdata.tm.module.dto;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 把「刚导入的一批 Module」聚合成落地工作项（纯函数）。TAP-12057 · P3-1（方案 §3.4 / <b>ADR-0002</b>）。
 *
 * <p>时序上它接在 {@code dataSourceService.batchImport} 之后——连接已经在目标环境落好、conMap 已建立，
 * 这时才谈得上「索引建到哪个库」。目标连接<b>只能</b>从 conMap 拿：跨环境不能按 {@code qualifiedName} 匹配
 * （内嵌逐环境不同的 id），更不能在 TM 侧用 {@code MongoTemplate} 自己连（那是平台自有库，错库建索引不报错）。</p>
 *
 * <p><b>解析口径</b>：conMap 的键是<b>导出侧（旧）连接 id</b>、值是目标环境连接 DTO（见
 * {@code DataSourceServiceImpl#batchImport}），与 {@code ModulesService#updateConnectionIds} 同一把钥匙。
 * 但 {@code modulesService.batchImport} 会<b>就地改写</b> Module 的 {@code connectionId} 为目标 id，
 * 故按键查不到时再按 conMap 的<b>值</b>兜一次——这样本聚合器放在改写前后都成立。
 * <b>不做按名匹配兜底</b>：同名连接可能属于不同用户，匹配错等于建到别人的库（导入侧早已弃用名匹配）。</p>
 *
 * <p>纯函数、无副作用、不触库不触 Spring：可离线 TDD。去重不在这里——并集后的重复声明由
 * {@link ServingIndexLandingPlanner} 按签名合并，全项目只有那一份判等。</p>
 */
public final class ServingIndexLandingTargets {

	/** 桶键分隔符；连接 id 是定长 24 位十六进制，加它只为可读。 */
	private static final String KEY_SEPARATOR = "::";

	private ServingIndexLandingTargets() {
	}

	/**
	 * @param modules 本次导入的 Module（只看它们的 {@code servingIndexes}）
	 * @param conMap  {@code 导出侧连接 id → 目标环境连接 DTO}
	 * @return 可落地的 {@code (连接, 集合)} 工作项 + 有声明却落不了地的记录
	 */
	public static ServingIndexLandingWorkList from(List<ModulesDto> modules,
												   Map<String, DataSourceConnectionDto> conMap) {
		ServingIndexLandingWorkList work = new ServingIndexLandingWorkList();
		if (modules == null || modules.isEmpty()) {
			return work;
		}
		Map<String, DataSourceConnectionDto> byTargetId = indexByTargetId(conMap);
		Map<String, ServingIndexLandingTarget> byBucket = new LinkedHashMap<>();

		for (ModulesDto module : modules) {
			if (module == null) {
				continue;
			}
			// 只有声明项才是「本 API 要的索引」；读回后未勾选的仅存着知悉，不能去建（ADR-0011 D2）。
			List<ServingIndex> declared = ServingIndexes.collectedOnly(module.getServingIndexes());
			if (declared.isEmpty()) {
				// 无声明 = 无事可做：连接/表名解析不出来也不报，免得拿无关 API 拖累整次导入。
				continue;
			}
			String connectionId = resolveConnectionId(module);
			DataSourceConnectionDto connection = resolveConnection(connectionId, conMap, byTargetId);
			if (connection == null || connection.getId() == null) {
				work.getUnresolved().add(unresolved(module, connectionId, declared.size(),
						UnresolvedServingIndexTarget.Reason.CONNECTION_UNRESOLVED));
				continue;
			}
			String tableName = module.getTableName();
			if (isBlank(tableName)) {
				work.getUnresolved().add(unresolved(module, connectionId, declared.size(),
						UnresolvedServingIndexTarget.Reason.TABLE_NAME_MISSING));
				continue;
			}

			String bucket = connection.getId().toHexString() + KEY_SEPARATOR + tableName;
			ServingIndexLandingTarget target = byBucket.get(bucket);
			if (target == null) {
				target = new ServingIndexLandingTarget(connection, tableName);
				byBucket.put(bucket, target);
				work.getTargets().add(target);
			}
			target.getDeclared().addAll(declared);
			if (!isBlank(module.getName())) {
				target.getSourceApis().add(module.getName());
				for (ServingIndex declaration : declared) {
					target.recordDeclaredBy(ServingIndexSignature.of(declaration), module.getName());
				}
			}
		}
		return work;
	}

	/** conMap 的值再按<b>目标</b> id 建一份索引，用于 Module 的 connectionId 已被改写成目标 id 的情形。 */
	private static Map<String, DataSourceConnectionDto> indexByTargetId(Map<String, DataSourceConnectionDto> conMap) {
		Map<String, DataSourceConnectionDto> byId = new LinkedHashMap<>();
		if (conMap == null) {
			return byId;
		}
		for (DataSourceConnectionDto connection : conMap.values()) {
			if (connection != null && connection.getId() != null) {
				byId.putIfAbsent(connection.getId().toHexString(), connection);
			}
		}
		return byId;
	}

	private static DataSourceConnectionDto resolveConnection(String connectionId,
															Map<String, DataSourceConnectionDto> conMap,
															Map<String, DataSourceConnectionDto> byTargetId) {
		if (isBlank(connectionId) || conMap == null) {
			return null;
		}
		DataSourceConnectionDto hit = conMap.get(connectionId);
		return hit != null ? hit : byTargetId.get(connectionId);
	}

	/**
	 * Module 上的连接 id：{@code connectionId} → {@code connection} → {@code dataSource}，与
	 * {@code ServingIndexLoadService#resolveConnectionId} 同序。三个字段本应由
	 * {@code ModulesService#alignConnectionWithDataSource} 对齐，但导出包里的 {@code connection}
	 * 曾被序列化成对象形态、回读得到 null，所以逐个兜。
	 */
	private static String resolveConnectionId(ModulesDto module) {
		if (!isBlank(module.getConnectionId())) {
			return module.getConnectionId();
		}
		if (module.getConnection() != null) {
			return module.getConnection().toHexString();
		}
		return isBlank(module.getDataSource()) ? null : module.getDataSource();
	}

	private static UnresolvedServingIndexTarget unresolved(ModulesDto module, String connectionId, int indexCount,
														   UnresolvedServingIndexTarget.Reason reason) {
		return new UnresolvedServingIndexTarget(module.getName(),
				module.getId() == null ? null : module.getId().toHexString(),
				connectionId, module.getTableName(), indexCount, reason);
	}

	private static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}
}
