package com.tapdata.tm.servingindex;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.module.dto.ModulesDto;
import com.tapdata.tm.module.dto.ServingIndexLandingPlan;
import com.tapdata.tm.module.dto.ServingIndexLandingReport;
import com.tapdata.tm.module.dto.ServingIndexLandingReports;
import com.tapdata.tm.module.dto.ServingIndexLandingPlanner;
import com.tapdata.tm.module.dto.ServingIndexLandingSummary;
import com.tapdata.tm.module.dto.ServingIndexLandingTarget;
import com.tapdata.tm.module.dto.ServingIndexLandingTargets;
import com.tapdata.tm.module.dto.ServingIndexTargetOutcome;
import com.tapdata.tm.module.dto.ServingIndexLandingWorkList;
import com.tapdata.tm.module.dto.UnresolvedServingIndexTarget;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * P3-1/P3-3 · TM 侧「服务型索引」落地编排与幂等执行。TAP-12057（方案 §3.4 / <b>ADR-0002</b> / <b>ADR-0005</b>）。
 *
 * <p>下一环境导入 API 时，Module 里带着上一环境收录的索引声明（{@code servingIndexes}，ADR-0001）。
 * 本服务接在 {@code dataSourceService.batchImport} 之后被调用——那时连接已在目标环境就位、conMap 已建立
 * ——把这批 Module 归并成 {@code (目标连接, 集合)} 工作项：<b>索引本体按集合建，所以桶必须是这个粒度</b>，
 * 多个 API 共表则声明取并集。</p>
 *
 * <p>逐个工作项<b>先读回、自写比对、只建缺的那一桶</b>（P3-3，见 {@link #apply}）；读回经
 * {@link ServingIndexRendezvous} 这条会合点回到 TM（<b>ADR-0012 D1</b>——CICD 腿没有前端会话）。</p>
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

	/** 读回等待上限：引擎读一张表的索引是毫秒级操作，30s 足够覆盖排队与网络抖动。 */
	static final long DEFAULT_READBACK_TIMEOUT_MILLIS = 30_000L;
	/** 建索引等待上限：存量集合上建索引可能耗时，给到 5 分钟（连接器用 background 建）。 */
	static final long DEFAULT_CREATE_TIMEOUT_MILLIS = 300_000L;

	private static final int TOKEN_PREFIX_LENGTH = ServingIndexRendezvous.TOKEN_PREFIX.length();

	private final ServingIndexService servingIndexService;
	private final ServingIndexRendezvous rendezvous;
	private final long readbackTimeoutMillis;
	private final long createTimeoutMillis;

	@Autowired
	public ServingIndexLandingService(ServingIndexService servingIndexService, ServingIndexRendezvous rendezvous) {
		this(servingIndexService, rendezvous, DEFAULT_READBACK_TIMEOUT_MILLIS, DEFAULT_CREATE_TIMEOUT_MILLIS);
	}

	ServingIndexLandingService(ServingIndexService servingIndexService, ServingIndexRendezvous rendezvous,
							   long readbackTimeoutMillis, long createTimeoutMillis) {
		this.servingIndexService = servingIndexService;
		this.rendezvous = rendezvous;
		this.readbackTimeoutMillis = readbackTimeoutMillis;
		this.createTimeoutMillis = createTimeoutMillis;
	}

	/**
	 * 为刚导入的一批 Module 规划索引落地。
	 *
	 * <p>解析目标连接 → 归并 {@code (连接, 集合)} 工作项（P3-1）→ 逐项幂等落地（P3-3）。
	 * {@code unresolved} 与逐项失败都<b>只记不抛</b>，汇总与响亮报错由 <b>P3-5</b> 落。</p>
	 *
	 * @param importedModules 本次导入的 Module（声明就在它们的 {@code servingIndexes} 上）
	 * @param conMap          {@code 导出侧连接 id → 目标环境连接 DTO}（导入现场那一张，不要另建）
	 * @param user            发起用户；P3-3 下发引擎动作时按它解析 worker，故此处即纳入签名，
	 *                        免得届时再改这个被多处导入路径共用的调用点
	 * @return 工作项 + 落不了地的记录 + 逐项落地结果（<b>都不吞</b>）
	 */
	public ServingIndexLandingWorkList landAfterImport(List<ModulesDto> importedModules,
													   Map<String, DataSourceConnectionDto> conMap,
													   UserDetail user) {
		return land(importedModules, conMap, user, true);
	}

	private ServingIndexLandingWorkList land(List<ModulesDto> importedModules,
											 Map<String, DataSourceConnectionDto> conMap,
											 UserDetail user, boolean execute) {
		ServingIndexLandingWorkList work = ServingIndexLandingTargets.from(importedModules, conMap);
		// 无条件先打一行：导入是低频动作，刷不了屏；而「没被调用」与「调用了但没声明可落」
		// 若都不打日志就在现场无从分辨——2026-08-01 实机查「为什么没日志」时正是卡在这。
		log.info("serving index landing planned, modules = {}, conMap = {}, targets = {}, declared = {}, unresolved = {}",
				importedModules == null ? 0 : importedModules.size(), conMap == null ? 0 : conMap.size(),
				work.getTargets().size(), work.declaredCount(), work.getUnresolved().size());
		if (work.isEmpty()) {
			return work;
		}

		for (ServingIndexLandingTarget target : work.getTargets()) {
			log.info("serving index landing target, connection = {}({}), table = {}, declared = {}, from = {}",
					target.getConnectionName(), target.getConnectionId(), target.getTableName(),
					target.getDeclared().size(), target.getSourceApis());
		}
		for (UnresolvedServingIndexTarget gap : work.getUnresolved()) {
			// error 而非 warn（P3-5）：目标连接猜不出来就不能建索引，猜错等于建到别的库（ADR-0002）；
			// 这条要在 CICD 现场一眼看见——warn 淹在导入的流水日志里，和静默跳过没区别。
			log.error("serving index landing unresolved, api = {}, reason = {}, connectionId = {}, table = {}, indexes = {}",
					gap.getApiName(), gap.getReason(), gap.getConnectionId(), gap.getTableName(), gap.getIndexCount());
		}

		// 逐 target 执行，不首错即停：一个集合塌了不该拖累其余集合（P3-5 汇总口径）。
		for (ServingIndexLandingTarget target : work.getTargets()) {
			work.getOutcomes().add(apply(target, user, execute));
		}
		summarise(work);
		return work;
	}

	/**
	 * P3-5 · <b>收集汇总</b>——「不首错即停」的另一半。
	 *
	 * <p>逐 target 各走各的之后，得有一处把「建了什么 / 谁没建成 / 谁根本落不了地」一次说清：
	 * 失败散在几十行日志里等于没报，而现场翻 CICD 日志的人往往只看得见最后这一行。
	 * 权限不足在 {@link ServingIndexLandingSummary#describe()} 里单独点名——它是唯一「改个授权就能恢复」
	 * 的常见失败，重跑幂等（方案 §4）。</p>
	 */
	private void summarise(ServingIndexLandingWorkList work) {
		ServingIndexLandingSummary summary = ServingIndexLandingSummary.of(work);
		if (summary.isClean()) {
			log.info("serving index landing summary, {}", summary.describe());
			return;
		}
		log.error("serving index landing summary, {}; problems = {}", summary.describe(), summary.getProblems());
	}

	/**
	 * P3-4 · <b>dry-run</b>：只读回 + 比对 + 出分桶报告，<b>一条索引都不建</b>。
	 *
	 * <p>与 {@link #landAfterImport} 走同一条比对路径——报告若和真跑的结果不一致，报告就没有价值。
	 * 唯一的差别是「将创建」那一桶不下发。</p>
	 */
	public ServingIndexLandingReport preview(List<ModulesDto> importedModules,
											 Map<String, DataSourceConnectionDto> conMap, UserDetail user) {
		return ServingIndexLandingReports.of(land(importedModules, conMap, user, false));
	}

	/**
	 * 一个 {@code (连接, 集合)} 的幂等落地：<b>先读回、自写比对，再只建「将创建」那一桶</b>。P3-3（ADR-0005）。
	 *
	 * <p>顺序不可颠倒、也不可省掉读回：连接器把 errorCode 85/86 catch 后 continue、报「成功」，
	 * 所以<b>绝不能</b>拿它当冲突判定——它只是第二道兜底（应对 query 与 create 之间的漂移窗口）。
	 * 读回拿不到就<b>不建</b>：把失败当成「目标没有索引」，在只加不删语义下就是往目标库重复建索引。</p>
	 */
	private ServingIndexTargetOutcome apply(ServingIndexLandingTarget target, UserDetail user, boolean execute) {
		ServingIndexTargetOutcome outcome = new ServingIndexTargetOutcome();
		outcome.setConnectionId(target.getConnectionId());
		outcome.setConnectionName(target.getConnectionName());
		outcome.setTableName(target.getTableName());
		outcome.getSourceApis().addAll(target.getSourceApis());

		String token = rendezvous.newToken();
		String readReqId = "si-land-read-" + token.substring(TOKEN_PREFIX_LENGTH);
		servingIndexService.sendQueryIndexes(target.getConnection(), target.getTableName(), readReqId, token, user);
		ServingIndexReadback readback = rendezvous.await(token, readReqId, readbackTimeoutMillis);
		if (!readback.isUsable()) {
			outcome.setError(readback.isTimedOut()
					? "read back timeout, engine offline or no reply"
					: readback.getError());
			log.warn("serving index landing aborted, table = {}, error = {}", target.getTableName(), outcome.getError());
			return outcome;
		}

		ServingIndexLandingPlan plan = ServingIndexLandingPlanner.plan(target.getDeclared(), readback.getIndexes());
		outcome.setPlan(plan);
		log.info("serving index landing plan, connection = {}, table = {}, create = {}, skip = {}, extra = {}",
				target.getConnectionName(), target.getTableName(),
				plan.getCreate().size(), plan.getSkip().size(), plan.getExtra().size());
		if (plan.getCreate().isEmpty() || !execute) {
			// create 为空 = 幂等成功（验收口径「第二次全部 skip」）；!execute = dry-run，比对到此为止。
			return outcome;
		}

		String createToken = rendezvous.newToken();
		String createReqId = "si-land-create-" + createToken.substring(TOKEN_PREFIX_LENGTH);
		servingIndexService.sendCreateIndexes(target.getConnection(), target.getTableName(), plan.getCreate(),
				createReqId, createToken, user);
		ServingIndexCreateAck ack = rendezvous.awaitCreateAck(createToken, createReqId, createTimeoutMillis);
		if (ack.isTimedOut()) {
			outcome.setError("create index timeout, engine offline or no reply");
		} else if (ack.getError() != null) {
			outcome.setError(ack.getError());
		}
		outcome.getCreated().addAll(ack.getCreated());
		outcome.getFailed().addAll(ack.getFailed());
		if (!outcome.isSucceeded()) {
			log.warn("serving index landing had failures, table = {}, error = {}, failed = {}",
					target.getTableName(), outcome.getError(), outcome.getFailed());
		}
		return outcome;
	}
}
