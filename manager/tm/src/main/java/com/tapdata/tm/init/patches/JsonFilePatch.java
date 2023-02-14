package com.tapdata.tm.init.patches;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONConfig;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.mongodb.client.ListIndexesIterable;
import com.tapdata.tm.init.MongoIndex;
import com.tapdata.tm.init.PatchConstant;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/16 21:07 Create
 */
public class JsonFilePatch extends AbsPatch {
    private static final Logger logger = LogManager.getLogger(JsonFilePatch.class);

    private final @NonNull String fileName;
    private final @NonNull String scriptStr;
    private final @NonNull MongoTemplate mongoTemplate;
    private static Map<String, String> replaceMap;
    static {
        replaceMap = new HashMap<>();
        replaceMap.put("TAPDATA.MONGODB.URI", PatchConstant.mongodbUri);
    }

    public JsonFilePatch(@NonNull PatchType type, @NonNull PatchVersion version, @NonNull String fileName, @NonNull String scriptStr) {
        super(type, version);
        this.fileName = fileName;
        this.scriptStr = scriptStr;
        this.mongoTemplate = SpringContextHelper.getBean(MongoTemplate.class);
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
                for (Document document : documents) {
                    existIndexMap.put((String) document.get("name"), JSONUtil.toBean(JSONUtil.toJsonStr(document), MongoIndex.class));
                }

                JSONArray jsonArray = (JSONArray) create.get("indexes");

                if (CollectionUtils.isEmpty(jsonArray)) {
                    continue;
                }
                JSONArray newJsonArray = new JSONArray();
                for (Object index : jsonArray) {
                    JSONObject jsonObject = (JSONObject) index;
                    MongoIndex mongoIndex = jsonObject.toBean(MongoIndex.class);
                    //查询索引是否存在，不存在不能删除
                    MongoIndex mongoIndex1 = existIndexMap.get(mongoIndex.getName());
                    if (!mongoIndex.equals(mongoIndex1)) {
                        if (mongoIndex1 != null && mongoIndex.getName().equals(mongoIndex1.getName())) {
                            Document drop = new Document();
                            drop.put("dropIndexes", collectionName);
                            drop.put("index", mongoIndex.getName());
                            needDrops.add(drop);

                        }
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

    public static String replaceScript(String script) {
        if (StringUtils.isBlank(script)) {
            return script;
        }
        String finalScript = script;
        String findKey = replaceMap.keySet().stream().filter(key -> finalScript.contains("${" + key + "}")).findFirst().orElse(null);
        if (null != findKey) {
            String replaceValue = replaceMap.get(findKey);
            if (null != replaceValue) {
                script = script.replaceAll("\\$\\{" + findKey + "}", replaceMap.get(findKey));
            }
        }
        return script;
    }
}
