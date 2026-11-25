package com.tapdata.tm.throttle;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.throttle.entity.AgentThrottleCapabilityEntity;
import com.tapdata.tm.throttle.entity.TaskThrottleChangeLogEntity;
import com.tapdata.tm.throttle.entity.TaskThrottlePolicyEntity;
import com.tapdata.tm.throttle.entity.TaskThrottleStopEvidenceEntity;
import com.tapdata.tm.throttle.entity.TaskThrottleTelemetryEntity;
import org.bson.Document;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.UpdateDefinition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Thread-safe in-memory MongoOperations test double supporting CAS updates, unique constraints,
 * pull/push/set/unset operators, sorting, and range queries without requiring a live Mongo daemon.
 */
final class InMemoryMongoOperations {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final Map<Class<?>, Map<String, Document>> tables = new ConcurrentHashMap<>();
    private final AtomicBoolean failChangeLogInserts = new AtomicBoolean(false);
    private final MongoOperations mock;

    @SuppressWarnings("unchecked")
    InMemoryMongoOperations() {
        this.mock = mock(MongoOperations.class);

        when(mock.insert(any(Object.class))).thenAnswer(inv -> {
            Object entity = inv.getArgument(0);
            insert(entity);
            return entity;
        });
        when(mock.findOne(any(Query.class), any(Class.class))).thenAnswer(inv -> {
            Query query = inv.getArgument(0);
            Class<?> clazz = inv.getArgument(1);
            return findOne(query, clazz);
        });
        when(mock.find(any(Query.class), any(Class.class))).thenAnswer(inv -> {
            Query query = inv.getArgument(0);
            Class<?> clazz = inv.getArgument(1);
            return find(query, clazz);
        });
        when(mock.findAll(any(Class.class))).thenAnswer(inv -> {
            Class<?> clazz = inv.getArgument(0);
            return findAll(clazz);
        });
        when(mock.findAndModify(any(Query.class), any(UpdateDefinition.class), any(FindAndModifyOptions.class), any(Class.class)))
                .thenAnswer(inv -> {
                    Query query = inv.getArgument(0);
                    UpdateDefinition update = inv.getArgument(1);
                    FindAndModifyOptions opts = inv.getArgument(2);
                    Class<?> clazz = inv.getArgument(3);
                    return findAndModify(query, update, opts, clazz);
                });
        when(mock.updateFirst(any(Query.class), any(UpdateDefinition.class), any(Class.class)))
                .thenAnswer(inv -> {
                    Query query = inv.getArgument(0);
                    UpdateDefinition update = inv.getArgument(1);
                    Class<?> clazz = inv.getArgument(2);
                    return updateFirst(query, update, clazz);
                });
        when(mock.upsert(any(Query.class), any(UpdateDefinition.class), any(Class.class)))
                .thenAnswer(inv -> {
                    Query query = inv.getArgument(0);
                    UpdateDefinition update = inv.getArgument(1);
                    Class<?> clazz = inv.getArgument(2);
                    return upsert(query, update, clazz);
                });
        when(mock.remove(any(Query.class), any(Class.class)))
                .thenAnswer(inv -> {
                    Query query = inv.getArgument(0);
                    Class<?> clazz = inv.getArgument(1);
                    return remove(query, clazz);
                });
    }

    MongoOperations ops() {
        return mock;
    }

    void setFailChangeLogInserts(boolean fail) {
        failChangeLogInserts.set(fail);
    }

    synchronized <T> void seedRawDocument(Class<T> clazz, T entity) {
        insert(entity);
    }

    synchronized void insert(Object entity) {
        if (entity == null) {
            return;
        }
        Class<?> clazz = entity.getClass();
        if (clazz == TaskThrottleChangeLogEntity.class && failChangeLogInserts.get()) {
            throw new IllegalStateException("Simulated ChangeLog persistence failure");
        }
        Document doc = toStoredDocument(entity, clazz);
        String id = String.valueOf(doc.get("_id"));
        Map<String, Document> table = tableFor(clazz);
        if (table.containsKey(id)) {
            throw new DuplicateKeyException("Duplicate _id " + id + " for " + clazz.getSimpleName());
        }
        if (clazz == TaskThrottleChangeLogEntity.class) {
            Object dedupKey = doc.get("dedupKey");
            if (dedupKey != null) {
                for (Document existing : table.values()) {
                    if (Objects.equals(existing.get("dedupKey"), dedupKey)) {
                        throw new DuplicateKeyException("Duplicate dedupKey " + dedupKey);
                    }
                }
            }
        }
        table.put(id, deepCopy(doc));
    }

    synchronized <T> T findOne(Query query, Class<T> clazz) {
        List<T> all = find(query, clazz);
        return all.isEmpty() ? null : all.get(0);
    }

    synchronized <T> List<T> findAll(Class<T> clazz) {
        return find(new Query(), clazz);
    }

    synchronized <T> List<T> find(Query query, Class<T> clazz) {
        Map<String, Document> table = tableFor(clazz);
        Document criteria = query == null ? new Document() : query.getQueryObject();
        List<Document> matched = new ArrayList<>();
        for (Document doc : table.values()) {
            if (matches(doc, criteria)) {
                matched.add(deepCopy(doc));
            }
        }
        if (query != null && !query.getSortObject().isEmpty()) {
            Document sortDoc = query.getSortObject();
            matched.sort((a, b) -> compareBySortDoc(a, b, sortDoc));
        }
        int skip = query != null ? (int) Math.max(0L, query.getSkip()) : 0;
        int limit = query != null && query.getLimit() > 0 ? query.getLimit() : Integer.MAX_VALUE;
        List<T> result = new ArrayList<>();
        for (int i = skip; i < matched.size() && result.size() < limit; i++) {
            result.add(toEntity(matched.get(i), clazz));
        }
        return result;
    }

    synchronized <T> T findAndModify(Query query, UpdateDefinition update, FindAndModifyOptions opts, Class<T> clazz) {
        Map<String, Document> table = tableFor(clazz);
        Document criteria = query == null ? new Document() : query.getQueryObject();
        for (Map.Entry<String, Document> entry : table.entrySet()) {
            if (matches(entry.getValue(), criteria)) {
                Document before = deepCopy(entry.getValue());
                Document after = deepCopy(entry.getValue());
                applyUpdate(after, update, clazz);
                table.put(entry.getKey(), after);
                boolean returnNew = opts != null && opts.isReturnNew();
                return toEntity(returnNew ? after : before, clazz);
            }
        }
        return null;
    }

    synchronized UpdateResult updateFirst(Query query, UpdateDefinition update, Class<?> clazz) {
        Map<String, Document> table = tableFor(clazz);
        Document criteria = query == null ? new Document() : query.getQueryObject();
        for (Map.Entry<String, Document> entry : table.entrySet()) {
            if (matches(entry.getValue(), criteria)) {
                Document doc = deepCopy(entry.getValue());
                applyUpdate(doc, update, clazz);
                table.put(entry.getKey(), doc);
                return UpdateResult.acknowledged(1L, 1L, null);
            }
        }
        return UpdateResult.acknowledged(0L, 0L, null);
    }

    synchronized UpdateResult upsert(Query query, UpdateDefinition update, Class<?> clazz) {
        Map<String, Document> table = tableFor(clazz);
        Document criteria = query == null ? new Document() : query.getQueryObject();
        for (Map.Entry<String, Document> entry : table.entrySet()) {
            if (matches(entry.getValue(), criteria)) {
                Document doc = deepCopy(entry.getValue());
                applyUpdate(doc, update, clazz);
                table.put(entry.getKey(), doc);
                return UpdateResult.acknowledged(1L, 1L, null);
            }
        }
        Document doc = new Document();
        for (Map.Entry<String, Object> e : criteria.entrySet()) {
            if (!(e.getValue() instanceof Map<?, ?>)) {
                doc.put(e.getKey(), normalizeVal(e.getValue()));
            }
        }
        applyUpdate(doc, update, clazz);
        Object idVal = doc.get("_id");
        if (idVal == null) {
            String idField = resolveIdField(clazz);
            idVal = doc.get(idField);
            if (idVal == null) {
                idVal = UUID.randomUUID().toString();
            }
            doc.put("_id", String.valueOf(idVal));
        }
        String idField = resolveIdField(clazz);
        if (!"_id".equals(idField) && !doc.containsKey(idField)) {
            doc.put(idField, doc.get("_id"));
        }
        table.put(String.valueOf(doc.get("_id")), doc);
        return UpdateResult.acknowledged(1L, 1L, null);
    }

    synchronized DeleteResult remove(Query query, Class<?> clazz) {
        Map<String, Document> table = tableFor(clazz);
        Document criteria = query == null ? new Document() : query.getQueryObject();
        long removed = 0L;
        for (String key : new ArrayList<>(table.keySet())) {
            if (matches(table.get(key), criteria)) {
                table.remove(key);
                removed++;
            }
        }
        return DeleteResult.acknowledged(removed);
    }

    private Map<String, Document> tableFor(Class<?> clazz) {
        return tables.computeIfAbsent(clazz, k -> new LinkedHashMap<>());
    }

    private Document toStoredDocument(Object entity, Class<?> clazz) {
        Map<String, Object> raw = MAPPER.convertValue(entity, MAP_TYPE);
        Document doc = new Document(raw);
        String idField = resolveIdField(clazz);
        Object idVal = doc.get("_id");
        if (idVal == null && idField != null) {
            idVal = doc.get(idField);
        }
        if (idVal == null) {
            idVal = UUID.randomUUID().toString();
        }
        String idStr = String.valueOf(idVal);
        doc.put("_id", idStr);
        if (idField != null && !"_id".equals(idField)) {
            doc.put(idField, idStr);
        }
        return doc;
    }

    private <T> T toEntity(Document doc, Class<T> clazz) {
        Document copy = deepCopy(doc);
        String idField = resolveIdField(clazz);
        if (idField != null && !"_id".equals(idField) && copy.get(idField) == null && copy.get("_id") != null) {
            copy.put(idField, copy.get("_id"));
        }
        return MAPPER.convertValue(copy, clazz);
    }

    private static String resolveIdField(Class<?> clazz) {
        if (clazz == AgentThrottleCapabilityEntity.class) {
            return "agentId";
        }
        if (clazz == TaskThrottlePolicyEntity.class
                || clazz == TaskThrottleChangeLogEntity.class
                || clazz == TaskThrottleStopEvidenceEntity.class
                || clazz == TaskThrottleTelemetryEntity.class) {
            return "id";
        }
        return "id";
    }

    @SuppressWarnings("unchecked")
    private void applyUpdate(Document target, UpdateDefinition update, Class<?> clazz) {
        if (update == null) {
            return;
        }
        Document upd = update.getUpdateObject();
        Object setObj = upd.get("$set");
        if (setObj instanceof Map<?, ?> setMap) {
            for (Map.Entry<?, ?> e : setMap.entrySet()) {
                String k = String.valueOf(e.getKey());
                Object v = convertForStorage(e.getValue());
                target.put(k, v);
                String idField = resolveIdField(clazz);
                if ("_id".equals(k) && idField != null) {
                    target.put(idField, v);
                } else if (idField != null && idField.equals(k)) {
                    target.put("_id", v);
                }
            }
        }
        Object unsetObj = upd.get("$unset");
        if (unsetObj instanceof Map<?, ?> unsetMap) {
            for (Object k : unsetMap.keySet()) {
                target.remove(String.valueOf(k));
            }
        }
        Object pushObj = upd.get("$push");
        if (pushObj instanceof Map<?, ?> pushMap) {
            for (Map.Entry<?, ?> e : pushMap.entrySet()) {
                String field = String.valueOf(e.getKey());
                List<Object> list = (List<Object>) target.computeIfAbsent(field, k -> new ArrayList<>());
                list.add(convertForStorage(e.getValue()));
            }
        }
        Object pullObj = upd.get("$pull");
        if (pullObj instanceof Map<?, ?> pullMap) {
            for (Map.Entry<?, ?> e : pullMap.entrySet()) {
                String field = String.valueOf(e.getKey());
                Object existing = target.get(field);
                if (existing instanceof List<?> existingList) {
                    Object pullCond = e.getValue();
                    List<Object> kept = new ArrayList<>();
                    for (Object item : existingList) {
                        if (!matchesPullCondition(item, pullCond)) {
                            kept.add(item);
                        }
                    }
                    target.put(field, kept);
                }
            }
        }
    }

    private static Object convertForStorage(Object val) {
        if (val == null || val instanceof String || val instanceof Number || val instanceof Boolean) {
            return val;
        }
        if (val instanceof Enum<?> en) {
            return en.name();
        }
        return MAPPER.convertValue(val, Object.class);
    }

    private static boolean matchesPullCondition(Object item, Object cond) {
        if (cond instanceof Map<?, ?> condMap && item instanceof Map<?, ?> itemMap) {
            for (Map.Entry<?, ?> e : condMap.entrySet()) {
                Object actual = itemMap.get(String.valueOf(e.getKey()));
                if (!equalsNormalized(actual, e.getValue())) {
                    return false;
                }
            }
            return true;
        }
        return equalsNormalized(item, cond);
    }

    private static boolean matches(Document doc, Document criteria) {
        if (doc == null) {
            return false;
        }
        if (criteria == null || criteria.isEmpty()) {
            return true;
        }
        for (Map.Entry<String, Object> e : criteria.entrySet()) {
            String key = e.getKey();
            Object expected = e.getValue();
            Object actual = doc.get(key);
            if ("_id".equals(key) && actual == null) {
                actual = doc.get("id");
            }
            if (!matchesField(actual, expected)) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesField(Object actual, Object expected) {
        if (expected instanceof Map<?, ?> opMap) {
            for (Map.Entry<?, ?> op : opMap.entrySet()) {
                String operator = String.valueOf(op.getKey());
                Object operand = op.getValue();
                if (!matchesOperator(actual, operator, operand)) {
                    return false;
                }
            }
            return true;
        }
        return equalsNormalized(actual, expected);
    }

    private static boolean matchesOperator(Object actual, String op, Object operand) {
        return switch (op) {
            case "$gt" -> compareNumbersOrStrings(actual, operand) > 0;
            case "$gte" -> compareNumbersOrStrings(actual, operand) >= 0;
            case "$lt" -> compareNumbersOrStrings(actual, operand) < 0;
            case "$lte" -> compareNumbersOrStrings(actual, operand) <= 0;
            case "$ne" -> !equalsNormalized(actual, operand);
            case "$in" -> {
                if (!(operand instanceof Collection<?> col)) {
                    yield false;
                }
                for (Object candidate : col) {
                    if (equalsNormalized(actual, candidate)) {
                        yield true;
                    }
                }
                yield false;
            }
            default -> false;
        };
    }

    private static int compareNumbersOrStrings(Object a, Object b) {
        if (a instanceof Number na && b instanceof Number nb) {
            return Long.compare(na.longValue(), nb.longValue());
        }
        if (a == null || b == null) {
            return a == b ? 0 : (a == null ? -1 : 1);
        }
        return String.valueOf(normalizeVal(a)).compareTo(String.valueOf(normalizeVal(b)));
    }

    private static boolean equalsNormalized(Object a, Object b) {
        Object na = normalizeVal(a);
        Object nb = normalizeVal(b);
        if (na instanceof Number numA && nb instanceof Number numB) {
            return numA.longValue() == numB.longValue()
                    && Double.compare(numA.doubleValue(), numB.doubleValue()) == 0;
        }
        return Objects.equals(na, nb);
    }

    private static Object normalizeVal(Object v) {
        return v instanceof Enum<?> en ? en.name() : v;
    }

    private static int compareBySortDoc(Document a, Document b, Document sortDoc) {
        for (Map.Entry<String, Object> e : sortDoc.entrySet()) {
            int dir = e.getValue() instanceof Number n ? n.intValue() : (Sort.Direction.ASC.name().equals(String.valueOf(e.getValue())) ? 1 : -1);
            int cmp = compareNumbersOrStrings(a.get(e.getKey()), b.get(e.getKey()));
            if (cmp != 0) {
                return dir >= 0 ? cmp : -cmp;
            }
        }
        return 0;
    }

    private Document deepCopy(Document src) {
        Map<String, Object> copied = MAPPER.convertValue(src, MAP_TYPE);
        return new Document(copied);
    }
}
