package com.tapdata.tm.audit;

import com.tapdata.tm.audit.query.AuditEventVo;
import com.tapdata.tm.audit.query.AuditOperationVo;
import com.tapdata.tm.audit.query.AuditQueryService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * L2：操作级结果聚合。批量部分成功后异步最终结果是「成功」，
 * 列表与详情必须保持「部分成功」，不能被最后一条事件覆盖（TAP-13018 F-08 / F-11）。
 */
@Timeout(10)
class AuditOutcomeAggregationIT {

	@Test
	@DisplayName("REQUEST=PARTIAL 后跟全部对象的 FINAL，结果仍为 PARTIAL 且 pending 结束")
	void should_keep_partial_when_final_event_succeeds() throws Exception {
		// 批量下每个对象各自上报最终结果；只有 s0 已受理，给它一条 FINAL 才算完成
		AuditEventVo request = event("e1", AuditPhase.REQUEST, AuditOutcome.PARTIAL, batch(2, 1, 1), 1000L);
		assertThat(request.getObjectId()).isNull();
		AuditEventVo fin = event("e2", AuditPhase.FINAL, AuditOutcome.SUCCESS, null, 2000L);
		fin.setObjectId("s0");
		AuditOperationVo op = aggregate(request, fin);
		assertThat(op.getOutcome()).isEqualTo(AuditOutcome.PARTIAL.name());
		assertThat(op.getFinalTimestamp()).isNotNull();
		assertThat(op.isPending()).isFalse();
	}

	@Test
	@DisplayName("批量只有部分对象给出最终结果时保持 pending，全部完成后结束")
	void should_stay_pending_until_every_accepted_object_finishes() throws Exception {
		AuditEventVo request = event("e1", AuditPhase.REQUEST, AuditOutcome.ACCEPTED, batch(2, 2, 0), 1000L);
		AuditEventVo onlyA = event("e2", AuditPhase.FINAL, AuditOutcome.SUCCESS, null, 2000L);
		onlyA.setObjectId("s0");
		AuditOperationVo partialDone = aggregate(request, onlyA);
		assertThat(partialDone.isPending()).isTrue();

		AuditEventVo bothDone = event("e3", AuditPhase.FINAL, AuditOutcome.SUCCESS, null, 3000L);
		bothDone.setObjectId("s1");
		AuditOperationVo finished = aggregate(request, onlyA, bothDone);
		assertThat(finished.isPending()).isFalse();
		assertThat(finished.getOutcome()).isEqualTo(AuditOutcome.SUCCESS.name());
	}

	@Test
	@DisplayName("混合最终结果：一个对象失败、一个成功，整体保持 PARTIAL")
	void should_remain_partial_when_batch_finals_disagree() throws Exception {
		AuditEventVo request = event("e1", AuditPhase.REQUEST, AuditOutcome.ACCEPTED, batch(2, 2, 0), 1000L);
		AuditEventVo failedA = event("e2", AuditPhase.FINAL, AuditOutcome.FAILED, null, 2000L);
		failedA.setObjectId("s0");
		AuditEventVo okB = event("e3", AuditPhase.FINAL, AuditOutcome.SUCCESS, null, 3000L);
		okB.setObjectId("s1");
		AuditOperationVo op = aggregate(request, failedA, okB);
		assertThat(op.getOutcome()).isEqualTo(AuditOutcome.PARTIAL.name());
		assertThat(op.isPending()).isFalse();
	}

	@Test
	@DisplayName("请求事件批量明细含失败项时，即使状态为 ACCEPTED 也聚合为 PARTIAL")
	void should_derive_partial_from_batch_items() throws Exception {
		AuditOperationVo op = aggregate(
				event("e1", AuditPhase.REQUEST, AuditOutcome.ACCEPTED, batch(3, 2, 1), 1000L),
				event("e2", AuditPhase.FINAL, AuditOutcome.SUCCESS, null, 2000L));
		assertThat(op.getOutcome()).isEqualTo(AuditOutcome.PARTIAL.name());
	}

	@Test
	@DisplayName("无失败项时采用最终结果；请求已受理且尚无最终结果时保持 ACCEPTED 且标记待最终结果")
	void should_use_final_or_accepted() throws Exception {
		AuditOperationVo finished = aggregate(
				event("e1", AuditPhase.REQUEST, AuditOutcome.ACCEPTED, batch(2, 2, 0), 1000L),
				event("e2", AuditPhase.FINAL, AuditOutcome.SUCCESS, null, 2000L));
		assertThat(finished.getOutcome()).isEqualTo(AuditOutcome.SUCCESS.name());

		AuditOperationVo pending = aggregate(
				event("e1", AuditPhase.REQUEST, AuditOutcome.ACCEPTED, null, 1000L));
		assertThat(pending.getOutcome()).isEqualTo(AuditOutcome.ACCEPTED.name());
		assertThat(pending.isPending()).isTrue();
	}

	@Test
	@DisplayName("最终结果为失败时以失败为准")
	void should_prefer_failed_final_result() throws Exception {
		AuditOperationVo op = aggregate(
				event("e1", AuditPhase.REQUEST, AuditOutcome.ACCEPTED, null, 1000L),
				event("e2", AuditPhase.FINAL, AuditOutcome.FAILED, null, 2000L));
		assertThat(op.getOutcome()).isEqualTo(AuditOutcome.FAILED.name());
	}

	private static AuditEventVo event(String id, AuditPhase phase, AuditOutcome outcome, AuditBatch batch, long ts) {
		AuditEventVo vo = new AuditEventVo();
		vo.setEventId(id);
		vo.setPhase(phase.name());
		vo.setOutcome(outcome.name());
		vo.setBatch(batch);
		vo.setTimestamp(new Date(ts));
		vo.setUserId("u1");
		return vo;
	}

	private static AuditBatch batch(int total, int success, int failed) {
		AuditBatch batch = new AuditBatch();
		for (int i = 0; i < success; i++) {
			batch.add(new AuditBatchItem("s" + i, "s" + i, AuditOutcome.SUCCESS, null));
		}
		for (int i = 0; i < failed; i++) {
			batch.add(new AuditBatchItem("f" + i, "f" + i, AuditOutcome.FAILED, "Task.StartFailed"));
		}
		assertThat(batch.getTotal()).isEqualTo(total);
		return batch;
	}

	private static AuditOperationVo aggregate(AuditEventVo... events) throws Exception {
		Method method = AuditQueryService.class.getDeclaredMethod("aggregate", String.class, List.class);
		method.setAccessible(true);
		return (AuditOperationVo) method.invoke(null, "op1|task.start", new java.util.ArrayList<>(List.of(events)));
	}
}
