package com.tapdata.tm.servingindex;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.ServingIndexLandingTarget;
import com.tapdata.tm.module.dto.ServingIndexLandingTargets;
import com.tapdata.tm.module.dto.ServingIndexLandingWorkList;
import com.tapdata.tm.module.dto.UnresolvedServingIndexTarget;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * P3-1 · TM 侧「服务型索引」落地编排。TAP-12057（方案 §3.4 / <b>ADR-0002</b> / <b>ADR-0005</b>）。
 *
 * <p>下一环境导入 API 时，Module 里带着上一环境收录的索引声明（{@code servingIndexes}，ADR-0001）。
 * 本服务接在 {@code dataSourceService.batchImport} 之后被调用——那时连接已在目标环境就位、conMap 已建立
 * ——把这批 Module 归并成 {@code (目标连接, 集合)} 工作项：<b>索引本体按集合建，所以桶必须是这个粒度</b>，
 * 多个 API 共表则声明取并集。</p>
 *
 * <p><b>本服务不做的三件事</b>：不判等（口径在 {@code ServingIndexLandingPlanner}，P3-2）、
 * 不连用户库（读写都在引擎侧经 PDK，P1 已交付）、不删目标多出的索引（只加不删，ADR-0005）。</p>
 *
 * <p><b>两库红线</b>（ADR-0002）：本包（{@code com.tapdata.tm.servingindex..}）<b>绝不</b>依赖
 * {@code MongoTemplate}——它连的是平台自有库，错库建索引不报错却全表扫描；由
 * {@code ServingIndexPackageRedlineArchTest} 编译期强制。</p>
 */
@Service
@Slf4j
public class ServingIndexLandingService {

	/**
	 * 为刚导入的一批 Module 规划索引落地。
	 *
	 * <p><b>当前范围（P3-1）</b>：解析目标连接 + 归并工作项 + 记账。逐项的
	 * {@code QueryIndexes → 比对 → 创建/跳过} 由 <b>P3-3</b> 接在本方法里（工作项即它的输入），
	 * {@code unresolved} 的响亮报错与逐项失败汇总由 <b>P3-5</b> 落。</p>
	 *
	 * @param importedModules 本次导入的 Module（声明就在它们的 {@code servingIndexes} 上）
	 * @param conMap          {@code 导出侧连接 id → 目标环境连接 DTO}（导入现场那一张，不要另建）
	 * @param user            发起用户；P3-3 下发引擎动作时按它解析 worker，故此处即纳入签名，
	 *                        免得届时再改这个被多处导入路径共用的调用点
	 * @return 可落地的工作项 + 有声明却落不了地的记录（<b>都不吞</b>）
	 */
	public ServingIndexLandingWorkList landAfterImport(List<ModulesDto> importedModules,
													   Map<String, DataSourceConnectionDto> conMap,
													   UserDetail user) {
		ServingIndexLandingWorkList work = ServingIndexLandingTargets.from(importedModules, conMap);
		if (work.isEmpty()) {
			// 绝大多数导入不带索引声明：不打日志，免得刷屏。
			return work;
		}

		log.info("serving index landing planned, targets = {}, declared = {}, unresolved = {}",
				work.getTargets().size(), work.declaredCount(), work.getUnresolved().size());
		for (ServingIndexLandingTarget target : work.getTargets()) {
			log.info("serving index landing target, connection = {}({}), table = {}, declared = {}, from = {}",
					target.getConnectionName(), target.getConnectionId(), target.getTableName(),
					target.getDeclared().size(), target.getSourceApis());
		}
		for (UnresolvedServingIndexTarget gap : work.getUnresolved()) {
			// warn 而非静默：目标连接猜不出来就不能建索引，猜错等于建到别的库（ADR-0002）。
			log.warn("serving index landing unresolved, api = {}, reason = {}, connectionId = {}, table = {}, indexes = {}",
					gap.getApiName(), gap.getReason(), gap.getConnectionId(), gap.getTableName(), gap.getIndexCount());
		}
		return work;
	}
}
