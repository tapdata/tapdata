package com.tapdata.tm.notification.delivery.service;

import com.tapdata.tm.notification.delivery.constant.DeliveryStatus;
import com.tapdata.tm.notification.delivery.dto.DeliverySummaryVo;
import org.bson.Document;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;

import java.util.Arrays;
import java.util.List;

/**
 * 在聚合管道内计算投递汇总，与 {@link NotificationDeliveryServiceImpl#summarize} 的判定一致，
 * 使按投递状态、渠道的筛选能在库内完成再分页。只使用 MongoDB 4.0 起支持的操作符。
 */
public final class DeliveryAggregation {
    /** 汇总状态，取值同 {@link DeliverySummaryVo#getStatus()} */
    public static final String STATUS = "_deliveryStatus";
    /** 有效投递涉及的渠道 */
    public static final String CHANNELS = "_deliveryChannels";
    /** 有效投递条数 */
    public static final String TOTAL = "_deliveryTotal";

    private static final String VALID = "_deliveryValid";
    private static final String SUCCESS = "_deliverySuccess";
    private static final String FAILED = "_deliveryFailed";

    private DeliveryAggregation() {
    }

    public static AggregationOperation stage(String operator, Object body) {
        Document stage = new Document(operator, body);
        return context -> stage;
    }

    /** 关联投递记录：localField 为空值时 Mongo 会匹配缺字段的文档，调用方须保证该字段始终有值 */
    public static AggregationOperation lookup(String from, String localField, String foreignField, String as) {
        return stage("$lookup", new Document("from", from).append("localField", localField)
                .append("foreignField", foreignField).append("as", as));
    }

    /** 由投递数组字段计算 {@link #STATUS}、{@link #CHANNELS}、{@link #TOTAL} */
    public static List<AggregationOperation> summarize(String deliveriesField) {
        Document valid = new Document("$filter", new Document("input", "$" + deliveriesField).append("as", "d")
                .append("cond", new Document("$and", Arrays.asList(
                        new Document("$ne", Arrays.asList(new Document("$ifNull", Arrays.asList("$$d.channel", null)), null)),
                        new Document("$ne", Arrays.asList(new Document("$ifNull", Arrays.asList("$$d.status", null)), null))))));
        Document counts = new Document(TOTAL, new Document("$size", "$" + VALID))
                .append(SUCCESS, countStatus(DeliveryStatus.SUCCESS))
                .append(FAILED, countStatus(DeliveryStatus.FAILED))
                .append(CHANNELS, new Document("$setUnion", Arrays.asList(
                        new Document("$map", new Document("input", "$" + VALID).append("as", "d").append("in", "$$d.channel")),
                        Arrays.asList())));
        Document status = new Document("$switch", new Document("branches", Arrays.asList(
                branch(new Document("$eq", Arrays.asList("$" + TOTAL, 0)), DeliverySummaryVo.NONE),
                branch(new Document("$and", Arrays.asList(zero(FAILED), positive(SUCCESS))), DeliverySummaryVo.SUCCESS),
                branch(zero(FAILED), DeliverySummaryVo.SKIPPED),
                branch(positive(SUCCESS), DeliverySummaryVo.PARTIAL_FAILED)))
                .append("default", DeliverySummaryVo.FAILED));
        return Arrays.asList(
                stage("$addFields", new Document(VALID, valid)),
                stage("$addFields", counts),
                stage("$addFields", new Document(STATUS, status)));
    }

    private static Document countStatus(DeliveryStatus status) {
        return new Document("$size", new Document("$filter", new Document("input", "$" + VALID).append("as", "d")
                .append("cond", new Document("$eq", Arrays.asList("$$d.status", status.name())))));
    }

    private static Document branch(Document when, String then) {
        return new Document("case", when).append("then", then);
    }

    private static Document zero(String field) {
        return new Document("$eq", Arrays.asList("$" + field, 0));
    }

    private static Document positive(String field) {
        return new Document("$gt", Arrays.asList("$" + field, 0));
    }
}
