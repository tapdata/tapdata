package com.tapdata.tm.init.patches;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONConfig;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.mongodb.client.ListIndexesIterable;
import com.tapdata.tm.init.PatchType;
import com.tapdata.tm.init.PatchVersion;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.SpringContextHelper;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/16 21:07 Create
 */
public class JsonFilePatch extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(JsonFilePatch.class);

    private final @NonNull String fileName;
    private final @NonNull String scriptStr;
    private final MongoTemplate mongoTemplate;
    private final Map<String, String> allVariables;

    public JsonFilePatch(@NonNull PatchType type, @NonNull PatchVersion version, @NonNull String fileName, @NonNull String scriptStr, @NonNull Map<String, String> allVariables) {
        this(type, version, fileName, scriptStr, allVariables, SpringContextHelper.getBean(MongoTemplate.class));
    }

    JsonFilePatch(@NonNull PatchType type, @NonNull PatchVersion version, @NonNull String fileName, @NonNull String scriptStr, @NonNull Map<String, String> allVariables, MongoTemplate mongoTemplate) {
        super(type, version);
        this.fileName = fileName;
        this.scriptStr = scriptStr;
        this.mongoTemplate = mongoTemplate;
        this.allVariables = allVariables;
    }

    @Override
    public String patchId() {
        return "script:" + fileName;
    }

    @Override
    public void run() {
        logger.info("Execute json patch: {}...", fileName);
        try {
            List<JSONObject> inserts = new ArrayList<>();
            List<JSONObject> creates = new ArrayList<>();
            List<JSONObject> drops = new ArrayList<>();
            List<JSONObject> deletes = new ArrayList<>();
            List<JSONObject> updates = new ArrayList<>();
            JSONConfig jsonConfig = JSONConfig.create().setOrder(true);
            JSONArray objects = JSONUtil.parseArray(scriptStr, jsonConfig);
            for (Object object : objects) {
                JSONObject object1 = (JSONObject) object;
                Object insert = object1.get("insert");
                Object drop = object1.get("dropIndexes");
                Object create = object1.get("createIndexes");
                Object delete = object1.get("delete");
                Object update = object1.get("update");

                if (insert != null) {
                    inserts.add(object1);
                } else if (create != null) {
                    creates.add(object1);
                } else if (drop != null) {
                    drops.add(object1);
                } else if (delete != null) {
                    deletes.add(object1);
                } else if (update != null) {
                    updates.add(object1);
                }
            }

            if (CollectionUtils.isNotEmpty(updates)) {
                for (JSONObject update : updates) {
                    executeCommand(update.toString());
                }
            }

            if (CollectionUtils.isNotEmpty(deletes)) {
                for (JSONObject delete : deletes) {
                    executeCommand(delete.toString());
                }

            }
            List<JSONObject> newInserts = new ArrayList<>();
            for (JSONObject insert : inserts) {

                String collectionName = (String) insert.get("insert");
                JSONArray documents = (JSONArray) insert.get("documents");
                JSONArray newJsonArray = new JSONArray();
                if (CollectionUtils.isEmpty(documents)) {
                    continue;
                }
                for (Object document : documents) {
                    JSONObject document1 = (JSONObject) document;
                    Object idObj = document1.get("_id");
                    long id1 = 0;
                    if (idObj instanceof JSONObject) {
                        ObjectId id = new ObjectId((String) ((JSONObject) idObj).get("$oid"));
                        id1 = mongoTemplate.count(new Query(Criteria.where("_id").is(id)), collectionName);
                    } else {
                        id1 = mongoTemplate.count(new Query(Criteria.where("_id").is(idObj)), collectionName);
                    }
                    if (id1 == 0) {
                        newJsonArray.add(document);

                    }
                }

                if (newJsonArray.size() != 0) {
                    insert.set("documents", newJsonArray);
                    newInserts.add(insert);
                }

            }
            if (CollectionUtils.isNotEmpty(newInserts)) {
                for (JSONObject insert : newInserts) {
                    executeCommand(insert.toString());
                }
            }

            List<JSONObject> newDrops = new ArrayList<>();
            for (JSONObject drop : drops) {
                String collectionName = (String) drop.get("dropIndexes");
                Object object = drop.get("index");
                JSONArray jsonArray;
                ListIndexesIterable<Document> documents = mongoTemplate.getCollection(collectionName).listIndexes();
                List<String> existIndexName = new ArrayList<>();
                for (Document document : documents) {
                    existIndexName.add((String) document.get("name"));
                }
                if (object instanceof String) {
                    jsonArray = new JSONArray(Lists.of(object));
                } else {
                    jsonArray = (JSONArray) object;
                }
                if (CollectionUtils.isEmpty(jsonArray)) {
                    continue;
                }
                JSONArray newJsonArray = new JSONArray();
                for (Object index : jsonArray) {
                    //查询索引是否存在，不存在不能删除
                    String indexName = (String) index;
                    if (existIndexName.contains(indexName)) {
                        newJsonArray.add(indexName);
                    }
                }

                if (newJsonArray.size() != 0) {
                    drop.set("index", newJsonArray);
                    newDrops.add(drop);
                }
            }
            if (CollectionUtils.isNotEmpty(newDrops)) {
                for (JSONObject drop : newDrops) {
                    executeCommand(drop.toString());
                }
            }
            List<JSONObject> newCreates = new ArrayList<>();
            List<Document> needDrops = new ArrayList<>();
            Map<String, Set<String>> plannedDropsByCollection = new HashMap<>();

            for (JSONObject create : creates) {
                String collectionName = (String) create.get("createIndexes");
                ListIndexesIterable<Document> documents = mongoTemplate.getCollection(collectionName).listIndexes();
                Map<String, NormalizedIndex> existIndexMap = new HashMap<>();
                List<NormalizedIndex> existIndexes = new ArrayList<>();
                for (Document document : documents) {
                    NormalizedIndex existing = fromIndexMap(document);
                    existIndexMap.put(existing.name(), existing);
                    existIndexes.add(existing);
                }

                JSONArray jsonArray = (JSONArray) create.get("indexes");

                if (CollectionUtils.isEmpty(jsonArray)) {
                    continue;
                }
                Set<String> plannedDropNames = plannedDropsByCollection.computeIfAbsent(collectionName, k -> new HashSet<>());
                List<NormalizedIndex> targetIndexes = new ArrayList<>(jsonArray.size());
                for (Object index : jsonArray) {
                    JSONObject jsonObject = (JSONObject) index;
                    NormalizedIndex targetIndex = fromIndexMap(jsonObject);
                    targetIndexes.add(targetIndex);
                    NormalizedIndex existingByName = existIndexMap.get(targetIndex.name());
                    if (existingByName != null && !isIndexEquivalent(targetIndex, existingByName) && !"_id_".equals(targetIndex.name())) {
                        if (plannedDropNames.add(targetIndex.name())) {
                            Document drop = new Document();
                            drop.put("dropIndexes", collectionName);
                            drop.put("index", targetIndex.name());
                            needDrops.add(drop);
                        }
                    }
                }

                JSONArray newJsonArray = new JSONArray();
                for (int i = 0; i < jsonArray.size(); i++) {
                    Object index = jsonArray.get(i);
                    NormalizedIndex targetIndex = targetIndexes.get(i);
                    // 查询同名或等价索引是否已存在；等价索引直接跳过，并排除本轮已计划 drop 的索引
                    NormalizedIndex existingByName = existIndexMap.get(targetIndex.name());
                    if (existingByName != null) {
                        if (!isIndexEquivalent(targetIndex, existingByName)) {
                            newJsonArray.add(index);
                        }
                    } else if (existIndexes.stream()
                            .filter(existing -> !plannedDropNames.contains(existing.name()))
                            .noneMatch(existing -> isIndexEquivalent(targetIndex, existing))) {
                        newJsonArray.add(index);
                    }
                }

                if (newJsonArray.size() != 0) {
                    create.set("indexes", newJsonArray);
                    newCreates.add(create);
                }
            }

            if (CollectionUtils.isNotEmpty(needDrops)) {
                for (Document drop : needDrops) {
                    executeCommand(drop.toJson());
                }
            }

            if (CollectionUtils.isNotEmpty(newCreates)) {
                for (JSONObject create : newCreates) {
                    executeCommand(create.toString());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("execute script error", e);
        }
    }
    public void executeCommand(String scripts) {
        scripts = replaceScript(scripts);
        try {
            Document document = mongoTemplate.executeCommand(scripts);
            logger.info("executeCommand result: {}", document);
        } catch (Exception e) {
            logger.warn("execute scripts failed, scripts = "+ scripts, e);
        }
    }

    private String replaceScript(String script) {
        if (StringUtils.isBlank(script)) {
            return script;
        }
        String finalScript = script;
        String findKey = allVariables.keySet().stream().filter(key -> finalScript.contains("${" + key + "}")).findFirst().orElse(null);
        if (null != findKey) {
            String replaceValue = allVariables.get(findKey);
            if (null != replaceValue) {
                script = script.replaceAll("\\$\\{" + findKey + "}", allVariables.get(findKey));
            }
        }
        return script;
    }

    private record NormalizedIndex(
            String name,
            Document key,
            boolean unique,
            boolean sparse,
            boolean hidden,
            Object expireAfterSeconds,
            Document partialFilterExpression,
            Document weights,
            Document wildcardProjection,
            Document storageEngine,
            String defaultLanguage,
            String languageOverride,
            Object textIndexVersion,
            Object sphereIndexVersion,
            Object bits,
            Object min,
            Object max,
            Object bucketSize,
            Document collation
    ) {
    }

    static boolean isIndexEquivalent(NormalizedIndex target, NormalizedIndex existing) {
        if (target == null || existing == null) {
            return false;
        }
        return orderedDocumentEquals(target.key(), existing.key())
                && target.unique() == existing.unique()
                && target.sparse() == existing.sparse()
                && target.hidden() == existing.hidden()
                && normalizedValueEquals(target.expireAfterSeconds(), existing.expireAfterSeconds())
                && normalizedValueEquals(target.partialFilterExpression(), existing.partialFilterExpression())
                && normalizedValueEquals(target.weights(), existing.weights())
                && normalizedValueEquals(target.wildcardProjection(), existing.wildcardProjection())
                && normalizedValueEquals(target.storageEngine(), existing.storageEngine())
                && Objects.equals(target.defaultLanguage(), existing.defaultLanguage())
                && Objects.equals(target.languageOverride(), existing.languageOverride())
                && normalizedValueEquals(target.textIndexVersion(), existing.textIndexVersion())
                && normalizedValueEquals(target.sphereIndexVersion(), existing.sphereIndexVersion())
                && normalizedValueEquals(target.bits(), existing.bits())
                && normalizedValueEquals(target.min(), existing.min())
                && normalizedValueEquals(target.max(), existing.max())
                && normalizedValueEquals(target.bucketSize(), existing.bucketSize())
                && normalizedValueEquals(target.collation(), existing.collation());
    }

    private static NormalizedIndex fromIndexMap(Map<String, Object> source) {
        return new NormalizedIndex(
                toStringValue(source.get("name")),
                toOrderedDocument(source.get("key")),
                toBooleanValue(source.get("unique")),
                toBooleanValue(source.get("sparse")),
                toBooleanValue(source.get("hidden")),
                firstNonNull(source, "expireAfterSeconds"),
                toOrderedDocument(source.get("partialFilterExpression")),
                toOrderedDocument(source.get("weights")),
                toOrderedDocument(source.get("wildcardProjection")),
                toOrderedDocument(source.get("storageEngine")),
                toStringValue(firstNonNull(source, "default_language", "defaultLanguage")),
                toStringValue(firstNonNull(source, "language_override", "languageOverride")),
                firstNonNull(source, "textIndexVersion", "textVersion"),
                firstNonNull(source, "2dsphereIndexVersion", "sphereVersion"),
                firstNonNull(source, "bits"),
                firstNonNull(source, "min"),
                firstNonNull(source, "max"),
                firstNonNull(source, "bucketSize"),
                toOrderedDocument(source.get("collation"))
        );
    }

    private static Object firstNonNull(Map<String, Object> source, String... keys) {
        for (String key : keys) {
            Object value = source.get(key);
            if (!JSONUtil.isNull(value)) {
                return value;
            }
        }
        return null;
    }

    private static String toStringValue(Object value) {
        return JSONUtil.isNull(value) ? null : String.valueOf(value);
    }

    private static boolean toBooleanValue(Object value) {
        return Boolean.TRUE.equals(value);
    }

    private static Document toOrderedDocument(Object raw) {
        if (JSONUtil.isNull(raw)) {
            return null;
        }
        if (raw instanceof Map<?, ?> map) {
            Document result = new Document();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                result.put(String.valueOf(entry.getKey()), toOrderedValue(entry.getValue()));
            }
            return result;
        }
        return null;
    }

    private static Object toOrderedValue(Object value) {
        if (JSONUtil.isNull(value)) {
            return null;
        }
        if (value instanceof Map<?, ?>) {
            return toOrderedDocument(value);
        }
        if (value instanceof List<?> list) {
            List<Object> result = new ArrayList<>(list.size());
            for (Object item : list) {
                result.add(toOrderedValue(item));
            }
            return result;
        }
        return value;
    }

    private static boolean orderedDocumentEquals(Document a, Document b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null || a.size() != b.size()) {
            return false;
        }
        Iterator<Map.Entry<String, Object>> itA = a.entrySet().iterator();
        Iterator<Map.Entry<String, Object>> itB = b.entrySet().iterator();
        while (itA.hasNext() && itB.hasNext()) {
            Map.Entry<String, Object> entryA = itA.next();
            Map.Entry<String, Object> entryB = itB.next();
            if (!Objects.equals(entryA.getKey(), entryB.getKey())
                    || !normalizedValueEquals(entryA.getValue(), entryB.getValue())) {
                return false;
            }
        }
        return !itA.hasNext() && !itB.hasNext();
    }

    private static boolean normalizedValueEquals(Object a, Object b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Number numA && b instanceof Number numB) {
            BigDecimal bdA = toBigDecimal(numA);
            BigDecimal bdB = toBigDecimal(numB);
            if (bdA != null && bdB != null) {
                return bdA.compareTo(bdB) == 0;
            }
            return Double.compare(numA.doubleValue(), numB.doubleValue()) == 0;
        }
        if (a instanceof Map<?, ?> mapA && b instanceof Map<?, ?> mapB) {
            if (mapA.size() != mapB.size()) {
                return false;
            }
            for (Map.Entry<?, ?> entry : mapA.entrySet()) {
                Object key = entry.getKey();
                if (!mapB.containsKey(key) || !normalizedValueEquals(entry.getValue(), mapB.get(key))) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof List<?> listA && b instanceof List<?> listB) {
            if (listA.size() != listB.size()) {
                return false;
            }
            for (int i = 0; i < listA.size(); i++) {
                if (!normalizedValueEquals(listA.get(i), listB.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return Objects.equals(a, b);
    }

    private static BigDecimal toBigDecimal(Number number) {
        if (number instanceof BigDecimal bd) {
            return bd;
        }
        if (number instanceof Integer || number instanceof Long || number instanceof Short || number instanceof Byte) {
            return BigDecimal.valueOf(number.longValue());
        }
        if (number instanceof Double || number instanceof Float) {
            double value = number.doubleValue();
            return Double.isFinite(value) ? BigDecimal.valueOf(value) : null;
        }
        return null;
    }
}
