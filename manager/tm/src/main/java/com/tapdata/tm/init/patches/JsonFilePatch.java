package com.tapdata.tm.init.patches;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONConfig;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.mongodb.client.ListIndexesIterable;
import com.tapdata.tm.init.MongoIndex;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

            for (JSONObject create : creates) {
                String collectionName = (String) create.get("createIndexes");
                ListIndexesIterable<Document> documents = mongoTemplate.getCollection(collectionName).listIndexes();
                Map<String, MongoIndex> existIndexMap = new HashMap<>();
                List<MongoIndex> existIndexes = new ArrayList<>();
                for (Document document : documents) {
                    MongoIndex existing = fromIndexDocument(document);
                    existIndexMap.put(existing.getName(), existing);
                    existIndexes.add(existing);
                }

                JSONArray jsonArray = (JSONArray) create.get("indexes");

                if (CollectionUtils.isEmpty(jsonArray)) {
                    continue;
                }
                JSONArray newJsonArray = new JSONArray();
                for (Object index : jsonArray) {
                    JSONObject jsonObject = (JSONObject) index;
                    MongoIndex mongoIndex = fromIndexJson(jsonObject);
                    // 查询同名或等价索引是否已存在；等价索引直接跳过，避免升级重跑误 drop 重建
                    MongoIndex existingByName = existIndexMap.get(mongoIndex.getName());
                    if (existingByName != null) {
                        if (!isIndexEquivalent(mongoIndex, existingByName)) {
                            if (!"_id_".equals(mongoIndex.getName())) {
                                Document drop = new Document();
                                drop.put("dropIndexes", collectionName);
                                drop.put("index", mongoIndex.getName());
                                needDrops.add(drop);
                            }
                            newJsonArray.add(index);
                        }
                    } else if (existIndexes.stream().noneMatch(existing -> isIndexEquivalent(mongoIndex, existing))) {
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

    static boolean isIndexEquivalent(MongoIndex target, MongoIndex existing) {
        if (target == null || existing == null) {
            return false;
        }
        return orderedDocumentEquals(target.getKey(), existing.getKey())
                && target.isUnique() == existing.isUnique()
                && target.isSparse() == existing.isSparse()
                && target.isHidden() == existing.isHidden()
                && Objects.equals(target.getExpireAfterSeconds(), existing.getExpireAfterSeconds())
                && normalizedValueEquals(target.getPartialFilterExpression(), existing.getPartialFilterExpression())
                && normalizedValueEquals(target.getWeights(), existing.getWeights())
                && normalizedValueEquals(target.getWildcardProjection(), existing.getWildcardProjection())
                && normalizedValueEquals(target.getStorageEngine(), existing.getStorageEngine())
                && Objects.equals(target.getDefaultLanguage(), existing.getDefaultLanguage())
                && Objects.equals(target.getLanguageOverride(), existing.getLanguageOverride())
                && Objects.equals(target.getTextVersion(), existing.getTextVersion())
                && Objects.equals(target.getSphereVersion(), existing.getSphereVersion())
                && Objects.equals(target.getBits(), existing.getBits())
                && Objects.equals(target.getMin(), existing.getMin())
                && Objects.equals(target.getMax(), existing.getMax())
                && Objects.equals(target.getBucketSize(), existing.getBucketSize())
                && Objects.equals(target.getCollation(), existing.getCollation());
    }

    private static MongoIndex fromIndexDocument(Document document) {
        MongoIndex index = JSONUtil.toBean(JSONUtil.toJsonStr(document), MongoIndex.class);
        copySemanticDocuments(index, document);
        return index;
    }

    private static MongoIndex fromIndexJson(JSONObject jsonObject) {
        MongoIndex index = jsonObject.toBean(MongoIndex.class);
        copySemanticDocuments(index, jsonObject);
        return index;
    }

    private static void copySemanticDocuments(MongoIndex index, Map<String, Object> source) {
        index.setKey(toOrderedDocument(source.get("key")));
        index.setPartialFilterExpression(toOrderedDocument(source.get("partialFilterExpression")));
        index.setWeights(toOrderedDocument(source.get("weights")));
        index.setWildcardProjection(toOrderedDocument(source.get("wildcardProjection")));
        index.setStorageEngine(toOrderedDocument(source.get("storageEngine")));
    }

    private static Document toOrderedDocument(Object raw) {
        if (raw == null) {
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
