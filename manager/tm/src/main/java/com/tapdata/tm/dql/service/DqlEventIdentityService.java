package com.tapdata.tm.dql.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.dql.DqlRecordIdentityTypeEnum;
import com.tapdata.tm.dql.vo.DqlEventReportVo;
import com.tapdata.tm.dql.vo.DqlRecordSuccessReportVo;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Generates deterministic TM fallback identities while preserving authoritative Engine values.
 */
@Service
public class DqlEventIdentityService {
    private static final List<String> SOURCE_OFFSET_KEYS = List.of(
            "sourceoffset",
            "lsn",
            "oplogposition",
            "oplogoffset",
            "offset"
    );

    private final ObjectMapper canonicalObjectMapper;

    DqlEventIdentityService() {
        this(new ObjectMapper());
    }

    @Autowired
    public DqlEventIdentityService(ObjectMapper objectMapper) {
        this.canonicalObjectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null").copy()
                .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
                .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    public void fillPayloadHash(DqlEventReportVo report) {
        if (report != null && StringUtils.isBlank(report.getPayloadHash()) && report.getPayloadData() != null) {
            report.setPayloadHash(hash(report.getPayloadData()));
        }
    }

    public void fillIdentities(String taskId, DqlEventReportVo report) {
        if (report == null) {
            return;
        }
        fillPayloadHash(report);
        RecordIdentity recordIdentity = buildRecordIdentity(
                report.getRecordIdentity(),
                report.getRecordIdentityType(),
                report.getRecordIdentityFields(),
                report.getTableId(),
                report.getSourceTable(),
                report.getEventKey(),
                report.getPayloadHash()
        );
        report.setRecordIdentity(recordIdentity.identity());
        report.setRecordIdentityType(recordIdentity.type());
        report.setRecordIdentityFields(recordIdentity.fields());
        if (StringUtils.isNotBlank(report.getEventIdentity())) {
            return;
        }

        Map<?, ?> payload = asMap(report.getPayloadData());
        Object exactlyOnceId = value(payload, "exactlyonceid");
        if (hasValue(exactlyOnceId)) {
            report.setEventIdentity("eo:" + exactlyOnceId);
            return;
        }
        Object sourceOffset = sourceOffset(payload);
        if (hasValue(sourceOffset)) {
            report.setEventIdentity("offset:" + hash(sourceOffset));
            return;
        }

        if (hasKeyIdentity(report)) {
            Object keySource = report.getEventKey() == null || report.getEventKey().isEmpty()
                    ? report.getRecordIdentity()
                    : report.getEventKey();
            report.setEventIdentity(String.join(":",
                    "key",
                    component(report.getTaskRecordId()),
                    table(report.getTableId(), report.getSourceTable()),
                    component(report.getDmlType()),
                    component(report.getEventTime()),
                    hash(keySource),
                    component(report.getPayloadHash())
            ));
            return;
        }

        report.setEventIdentity(String.join(":",
                "payload",
                component(report.getTaskRecordId()),
                table(report.getTableId(), report.getSourceTable()),
                component(report.getDmlType()),
                component(report.getEventTime()),
                component(report.getPayloadHash()),
                component(report.getFailedNodeId())
        ));
    }

    public void fillRecordIdentity(DqlRecordSuccessReportVo report) {
        if (report == null) {
            return;
        }
        RecordIdentity identity = buildRecordIdentity(
                report.getRecordIdentity(),
                report.getRecordIdentityType(),
                report.getRecordIdentityFields(),
                report.getTableId(),
                report.getSourceTable(),
                report.getEventKey(),
                report.getPayloadHash()
        );
        report.setRecordIdentity(identity.identity());
        report.setRecordIdentityType(identity.type());
        report.setRecordIdentityFields(identity.fields());
    }

    private RecordIdentity buildRecordIdentity(String explicitIdentity,
                                               String explicitType,
                                               List<String> explicitFields,
                                               String tableId,
                                               String sourceTable,
                                               Map<String, Object> eventKey,
                                               String payloadHash) {
        if (StringUtils.isNotBlank(explicitIdentity)) {
            return new RecordIdentity(
                    explicitIdentity,
                    StringUtils.defaultIfBlank(explicitType, DqlRecordIdentityTypeEnum.UNKNOWN.name()),
                    explicitFields
            );
        }
        String table = table(tableId, sourceTable);
        if (eventKey != null && !eventKey.isEmpty()) {
            return new RecordIdentity(
                    "key:" + table + ":" + hash(eventKey),
                    DqlRecordIdentityTypeEnum.PRIMARY_KEY.name(),
                    eventKey.keySet().stream().sorted().toList()
            );
        }
        if (StringUtils.isNotBlank(payloadHash)) {
            return new RecordIdentity(
                    "hash:" + table + ":" + payloadHash,
                    DqlRecordIdentityTypeEnum.FULL_FIELD_HASH.name(),
                    List.of()
            );
        }
        return new RecordIdentity(null, DqlRecordIdentityTypeEnum.UNKNOWN.name(), List.of());
    }

    private boolean hasKeyIdentity(DqlEventReportVo report) {
        if (report.getEventKey() != null && !report.getEventKey().isEmpty()) {
            return true;
        }
        return DqlRecordIdentityTypeEnum.PRIMARY_KEY.name().equals(report.getRecordIdentityType())
                || DqlRecordIdentityTypeEnum.UNIQUE_INDEX.name().equals(report.getRecordIdentityType());
    }

    private Object sourceOffset(Map<?, ?> payload) {
        Map<?, ?> info = asMap(value(payload, "info"));
        if (info == null) {
            return null;
        }
        for (String key : SOURCE_OFFSET_KEYS) {
            Object offset = value(info, key);
            if (hasValue(offset)) {
                return offset;
            }
        }
        return null;
    }

    private Object value(Map<?, ?> source, String normalizedKey) {
        if (source == null) {
            return null;
        }
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            if (normalizeKey(entry.getKey()).equals(normalizedKey)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private String normalizeKey(Object key) {
        return String.valueOf(key).replace("_", "").replace("-", "").toLowerCase(Locale.ROOT);
    }

    private Map<?, ?> asMap(Object value) {
        return value instanceof Map<?, ?> map ? map : null;
    }

    private boolean hasValue(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof String string) {
            return StringUtils.isNotBlank(string);
        }
        if (value instanceof Map<?, ?> map) {
            return !map.isEmpty();
        }
        if (value instanceof Iterable<?> iterable) {
            return iterable.iterator().hasNext();
        }
        return true;
    }

    private String table(String tableId, String sourceTable) {
        return Optional.ofNullable(tableId).filter(StringUtils::isNotBlank)
                .orElseGet(() -> StringUtils.defaultString(sourceTable));
    }

    private String component(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private String hash(Object value) {
        try {
            byte[] canonicalBytes = canonicalObjectMapper.writeValueAsBytes(value);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(canonicalBytes);
            StringBuilder builder = new StringBuilder("sha256:");
            for (byte b : bytes) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (JsonProcessingException e) {
            throw new BizException("DqlEvent.InvalidPayload", e.getOriginalMessage());
        } catch (NoSuchAlgorithmException e) {
            throw new BizException("SystemError", e.getMessage());
        }
    }

    private record RecordIdentity(String identity, String type, List<String> fields) {
    }
}
