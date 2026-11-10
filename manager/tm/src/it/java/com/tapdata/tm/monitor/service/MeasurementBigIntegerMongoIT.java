package com.tapdata.tm.monitor.service;

import com.mongodb.client.MongoClients;
import com.tapdata.tm.commons.metrics.MetricCons;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.param.MeasurementQueryParam;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.common.sample.request.SampleRequest;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** Runs only against an explicitly supplied isolated MongoDB. */
@Timeout(30)
@EnabledIfSystemProperty(named = "measurement.it.mongoUri", matches = ".+")
class MeasurementBigIntegerMongoIT {
    @Test
    void writesLegacyUnsignedValueAsDecimalAndReadsItThroughInstantQuery() throws Exception {
        try (var client = MongoClients.create(System.getProperty("measurement.it.mongoUri"))) {
            MongoTemplate template = new MongoTemplate(client,
                    "measurement_review_" + UUID.randomUUID().toString().replace("-", ""));
            try {
                MeasurementServiceV2Impl service = new MeasurementServiceV2Impl(
                        CompletableFuture.completedFuture(template), null, null);
                BigInteger legacyValue = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
                Map<String, String> tags = Map.of(
                        MetricCons.Tags.F_TYPE, MetricCons.SampleType.TABLE.code(),
                        MetricCons.Tags.F_TASK_ID, "task-" + UUID.randomUUID());
                Sample sample = new Sample();
                sample.setVs(Map.of(MetricCons.SS.VS.F_SNAPSHOT_ROW_TOTAL, legacyValue));
                SampleRequest request = new SampleRequest();
                request.setTags(tags);
                request.setSample(sample);

                service.addAgentMeasurement(List.of(request));

                Document stored = template.getCollection(MeasurementEntity.COLLECTION_NAME).find().first();
                assertInstanceOf(Decimal128.class,
                        ((Document) stored.getList(MetricCons.F_SAMPLES, Document.class).get(0)
                                .get(Sample.FIELD_VALUES, Document.class))
                                .get(MetricCons.SS.VS.F_SNAPSHOT_ROW_TOTAL));
                MeasurementQueryParam.MeasurementQuerySample query = new MeasurementQueryParam.MeasurementQuerySample();
                query.setTags(tags);
                query.setFields(List.of(MetricCons.SS.VS.F_SNAPSHOT_ROW_TOTAL));
                query.setType(MeasurementQueryParam.MeasurementQuerySample.MEASUREMENT_QUERY_SAMPLE_TYPE_INSTANT);
                long now = System.currentTimeMillis();
                Map<String, Sample> result = service.getInstantSamples(query, "leftAndRight", now - 300_000, now + 300_000);
                assertFalse(result.isEmpty());
                assertEquals(legacyValue,
                        new BigDecimal(result.values().iterator().next().getVs()
                                .get(MetricCons.SS.VS.F_SNAPSHOT_ROW_TOTAL).toString()).toBigIntegerExact());
            } finally {
                template.getDb().drop();
            }
        }
    }
}
