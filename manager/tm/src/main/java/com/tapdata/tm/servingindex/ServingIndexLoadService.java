package com.tapdata.tm.servingindex;

import com.tapdata.tm.module.dto.ApiQueryPattern;
import com.tapdata.tm.module.dto.LoadedServingIndex;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.ServingIndex;
import com.tapdata.tm.module.dto.ServingIndexes;
import com.tapdata.tm.module.dto.ServingIndexLoadPlanner;
import com.tapdata.tm.module.dto.ServingIndexSignature;
import com.tapdata.tm.modules.service.ModulesService;
import com.tapdata.tm.utils.MongoUtils;
import io.tapdata.entity.schema.TapIndex;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * P2-2 · TM 加载服务端：把引擎读回的物理索引装配上归因/默认勾选上下文，交纯策略规划。TAP-12057（方案 §3.8）。
 *
 * <p>{@link ServingIndexService}（P1-2）负责<b>触发</b>读回（结果经 ws 回前端）；本服务负责读回<b>之后</b>的
 * 服务端规划：前端把读回的 {@code List<TapIndex>} 连同 {@code moduleId} 回传（P2-6 联调点），本服务据
 * moduleId 装配上下文——本 Module 的 Path（→ {@link ApiQueryPattern} 匹配判据）、本 Module 已收录签名、以及
 * 同 (连接,集合) 兄弟 Module 的「已收录签名 → 来源 API 名」——再喂给纯策略 {@link ServingIndexLoadPlanner}
 * （§3.8.3 首个命中即定）。全表可见、不预过滤（§3.8.1）。</p>
 *
 * <p><b>两库红线</b>（ADR-0002）：本服务只经 {@link ModulesService} 读<b>平台库</b>的 Module 元数据
 * （Module 本就是平台数据），<b>绝不</b>依赖 {@code MongoTemplate}、不在 TM 侧读写用户库索引
 * （索引本体读写在引擎侧 PDK，由 {@code ServingIndexPackageRedlineArchTest} 编译期强制）。</p>
 */
@Service
@Slf4j
public class ServingIndexLoadService {

	private final ModulesService modulesService;

	public ServingIndexLoadService(ModulesService modulesService) {
		this.modulesService = modulesService;
	}

	/**
	 * 规划读回索引的归因与默认勾选。
	 *
	 * @param moduleId 正在编辑的 API（Module）id
	 * @param indexes  引擎读回的该 (连接,集合) 全部物理索引
	 * @return 每条读回索引一条 {@link LoadedServingIndex}（保序、一条不少）
	 */
	public List<LoadedServingIndex> load(String moduleId, List<TapIndex> indexes) {
		ModulesDto thisModule = modulesService.findById(MongoUtils.toObjectId(moduleId));
		Assert.notNull(thisModule, "module is empty");

		ApiQueryPattern pattern = ApiQueryPattern.from(thisModule.getPaths());
		Set<String> thisApiSignatures = signaturesOf(thisModule.getServingIndexes());
		Map<String, String> otherApiSignatures = collectOtherApiSignatures(moduleId, thisModule);

		return ServingIndexLoadPlanner.plan(indexes, pattern, thisApiSignatures, otherApiSignatures);
	}

	/**
	 * 一个 Module 的<b>已声明</b>索引 → 身份签名集（{@link ServingIndexSignature}）。
	 *
	 * <p>只取 {@code collected=true}：Module 的 servingIndexes 里还存着「读回后未勾选、仅留在列表里」的，
	 * 那些不是收录，不能让归因把它们报成「已被本/他 API 收录」。</p>
	 */
	private Set<String> signaturesOf(List<ServingIndex> servingIndexes) {
		Set<String> out = new HashSet<>();
		for (ServingIndex si : ServingIndexes.collectedOnly(servingIndexes)) {
			out.add(ServingIndexSignature.of(si));
		}
		return out;
	}

	/**
	 * 扫同 (连接,集合) 的兄弟 Module，收「已收录签名 → 来源 API 名」。需<b>同表</b>、需<b>排除自身</b>；
	 * 同一签名被多个兄弟收录时，保留首个 API 名（标注只需一个来源，双写本无意义，§3.8.3）。
	 */
	private Map<String, String> collectOtherApiSignatures(String moduleId, ModulesDto thisModule) {
		Map<String, String> out = new HashMap<>();
		String connectionId = resolveConnectionId(thisModule);
		if (connectionId == null) {
			return out;
		}
		String tableName = thisModule.getTableName();
		for (ModulesDto sibling : modulesService.findByConnectionId(connectionId)) {
			if (sibling == null || isSelf(sibling, moduleId) || !sameTable(sibling, tableName)) {
				continue;
			}
			if (sibling.getServingIndexes() == null) {
				continue;
			}
			for (ServingIndex si : ServingIndexes.collectedOnly(sibling.getServingIndexes())) {
				out.putIfAbsent(ServingIndexSignature.of(si), sibling.getName());
			}
		}
		return out;
	}

	private static String resolveConnectionId(ModulesDto module) {
		if (module.getConnectionId() != null && !module.getConnectionId().trim().isEmpty()) {
			return module.getConnectionId();
		}
		return module.getConnection() != null ? module.getConnection().toHexString() : null;
	}

	private static boolean isSelf(ModulesDto sibling, String moduleId) {
		return sibling.getId() != null && sibling.getId().toHexString().equals(moduleId);
	}

	private static boolean sameTable(ModulesDto sibling, String tableName) {
		return tableName == null || tableName.equals(sibling.getTableName());
	}
}
