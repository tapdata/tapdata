//package com.tapdata.tm.init;
//
//import cn.hutool.core.comparator.VersionComparator;
//import cn.hutool.core.io.FileUtil;
//import cn.hutool.core.io.IORuntimeException;
//import cn.hutool.core.io.IoUtil;
//import cn.hutool.json.JSONArray;
//import cn.hutool.json.JSONConfig;
//import cn.hutool.json.JSONObject;
//import cn.hutool.json.JSONUtil;
//import com.mongodb.client.ListIndexesIterable;
//import com.tapdata.tm.utils.Lists;
//import com.tapdata.tm.verison.dto.VersionDto;
//import com.tapdata.tm.verison.service.VersionService;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.collections4.CollectionUtils;
//import org.bson.Document;
//import org.bson.types.ObjectId;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.boot.ApplicationArguments;
//import org.springframework.boot.ApplicationRunner;
//import org.springframework.core.annotation.Order;
//import org.springframework.core.io.Resource;
//import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
//import org.springframework.data.mongodb.core.MongoTemplate;
//import org.springframework.data.mongodb.core.query.Criteria;
//import org.springframework.data.mongodb.core.query.Query;
//import org.springframework.stereotype.Component;
//import org.springframework.util.ResourceUtils;
//
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.util.*;
//
//@Component
//@Order(Integer.MIN_VALUE)
//@Slf4j
//public class ScriptExecutorRunInit implements ApplicationRunner {
//
//  @Autowired
//  private MongoTemplate mongoTemplate;
//
//  @Autowired
//  private VersionService versionService;
//
//  @Value("#{'${spring.profiles.include:idaas}'.split(',')}")
//  private List<String> productList;
//  @Override
//  public void run(ApplicationArguments args) throws Exception {
////    executeScript("init", VersionDto.SCRIPT_VERSION_KEY);
////    log.info("Execute the initialization script to complete...");
//
//    executeScript("init/idaas", VersionDto.DAAS_SCRIPT_VERSION_KEY);
//    log.info("Execute the daas product initialization script to complete...");
//
//    if (productList.contains("dfs")) {
//      executeScript("init/dfs", VersionDto.DFS_SCRIPT_VERSION_KEY);
//      log.info("Execute the dfs product initialization script to complete...");
//    }
//    if (productList.contains("drs")) {
//      executeScript("init/drs", VersionDto.DRS_SCRIPT_VERSION_KEY);
//      log.info("Execute the drs product initialization script to complete...");
//    }
//
//  }
//
//  private void executeScript(String path, String scriptVersionKey) throws IOException {
//    PathMatchingResourcePatternResolver pathMatchingResourcePatternResolver = new PathMatchingResourcePatternResolver();
//    Resource versionRes = pathMatchingResourcePatternResolver.getResource(ResourceUtils.CLASSPATH_URL_PREFIX + path + "/version");
//    if (!versionRes.exists()) {
//      throw new RuntimeException("init script file does not exist");
//    }
//
//    String version;
//    try {
//      version = IoUtil.read(versionRes.getInputStream(), StandardCharsets.UTF_8);
//    } catch (IORuntimeException | IOException e) {
//      throw new RuntimeException("Error reading version file",e);
//    } finally {
//      IoUtil.close(versionRes.getInputStream());
//    }
//    log.info("The version of the latest init script is {}", version);
//
//
//    VersionDto dbVersionDto = versionService.findOne(Query.query(Criteria.where("type").is(scriptVersionKey)));
//    VersionComparator versionComparator = new VersionComparator();
//    if (dbVersionDto != null && versionComparator.compare(dbVersionDto.getVersion(), version) >= 0) {
//      log.info("The data version is up to date and does not need to be updated {}", dbVersionDto.getVersion());
//      return;
//    }
//    String dbVersion = dbVersionDto == null ? null : dbVersionDto.getVersion();
//    Resource[] resources = pathMatchingResourcePatternResolver
//            .getResources(ResourceUtils.CLASSPATH_URL_PREFIX + path + "/*.json");
//    log.info("The number of init script is {}", resources.length);
//    Arrays.sort(resources,
//            (r1,r2)-> versionComparator.compare(FileUtil.mainName(r1.getFilename()), FileUtil.mainName(r2.getFilename())));
//
//    for (Resource resource : resources) {
//      String currentVersion = FileUtil.mainName(resource.getFilename());
//      log.info("init script: {} - {}", resource.getFilename(),currentVersion);
//      if (versionComparator.compare(currentVersion, dbVersion) <= 0) {
//        log.info("The init script has been executed {}, skip...", currentVersion);
//        continue;
//      }
//      String scriptStr;
//      try {
//        scriptStr = IoUtil.read(resource.getInputStream(), StandardCharsets.UTF_8);
//      } catch (IORuntimeException | IOException e) {
//        throw new RuntimeException("Error reading script file", e);
//      } finally {
//        IoUtil.close(resource.getInputStream());
//      }
//      try {
//        runCommand(scriptStr);
//      } catch (Exception e) {
//        throw new RuntimeException("execute script error", e);
//      }
//      //update db version
//      VersionDto versionDto = new VersionDto(scriptVersionKey, currentVersion);
//      versionService.upsert(Query.query(Criteria.where("type").is(scriptVersionKey)), versionDto);
//      log.info("Update data script to version {} -> {}", dbVersion,  currentVersion);
//      dbVersion = currentVersion;
//
//    }
//  }
//
//
//  public void runCommand(String json) {
//    List<JSONObject> inserts = new ArrayList<>();
//    List<JSONObject> creates = new ArrayList<>();
//    List<JSONObject> drops = new ArrayList<>();
//    List<JSONObject> deletes = new ArrayList<>();
//    List<JSONObject> updates = new ArrayList<>();
//    JSONConfig jsonConfig = JSONConfig.create().setOrder(true);
//    JSONArray objects = JSONUtil.parseArray(json, jsonConfig);
//    for (Object object : objects) {
//      JSONObject object1 = (JSONObject) object;
//      Object insert = object1.get("insert");
//      Object drop = object1.get("dropIndexes");
//      Object create = object1.get("createIndexes");
//      Object delete = object1.get("delete");
//      Object update = object1.get("update");
//
//      if (insert != null) {
//        inserts.add(object1);
//      } else if (create != null) {
//        creates.add(object1);
//      } else if (drop != null) {
//        drops.add(object1);
//      } else if (delete != null) {
//        deletes.add(object1);
//      } else if (update != null) {
//        updates.add(object1);
//      }
//    }
//
//    if (CollectionUtils.isNotEmpty(updates)) {
//      for (JSONObject update : updates) {
//        executeCommand(update.toString());
//      }
//    }
//
//    if (CollectionUtils.isNotEmpty(deletes)) {
//      for (JSONObject delete : deletes) {
//        executeCommand(delete.toString());
//      }
//
//    }
//    List<JSONObject> newInserts = new ArrayList<>();
//    for (JSONObject insert : inserts) {
//
//      String collectionName = (String) insert.get("insert");
//      JSONArray documents = (JSONArray) insert.get("documents");
//      JSONArray newJsonArray = new JSONArray();
//      if (CollectionUtils.isEmpty(documents)) {
//        continue;
//      }
//      for (Object document : documents) {
//        JSONObject document1 = (JSONObject) document;
//        Object idObj =  document1.get("_id");
//        long id1 = 0;
//        if (idObj instanceof JSONObject) {
//          ObjectId id = new ObjectId((String) ((JSONObject) idObj).get("$oid"));
//          id1 = mongoTemplate.count(new Query(Criteria.where("_id").is(id)), collectionName);
//        } else {
//          id1 = mongoTemplate.count(new Query(Criteria.where("_id").is(idObj)), collectionName);
//        }
//        if (id1 == 0) {
//          newJsonArray.add(document);
//
//        }
//      }
//
//      if (newJsonArray.size() != 0) {
//        insert.set("documents", newJsonArray);
//        newInserts.add(insert);
//      }
//
//    }
//    if (CollectionUtils.isNotEmpty(newInserts)) {
//      for (JSONObject insert : newInserts) {
//        executeCommand(insert.toString());
//      }
//    }
//
//    List<JSONObject> newDrops = new ArrayList<>();
//    for (JSONObject drop : drops) {
//      String collectionName = (String) drop.get("dropIndexes");
//      Object object = drop.get("index");
//      JSONArray jsonArray;
//      ListIndexesIterable<Document> documents = mongoTemplate.getCollection(collectionName).listIndexes();
//      List<String> existIndexName = new ArrayList<>();
//      for (Document document : documents) {
//        existIndexName.add((String) document.get("name"));
//      }
//      if (object instanceof String) {
//        jsonArray = new JSONArray(Lists.of(object));
//      } else {
//        jsonArray = (JSONArray)object;
//      }
//      if (CollectionUtils.isEmpty(jsonArray)) {
//        continue;
//      }
//      JSONArray newJsonArray = new JSONArray();
//      for (Object index : jsonArray) {
//        //查询索引是否存在，不存在不能删除
//        String indexName = (String) index;
//        if (existIndexName.contains(indexName)) {
//          newJsonArray.add(indexName);
//        }
//      }
//
//      if (newJsonArray.size() != 0) {
//        drop.set("index", newJsonArray);
//        newDrops.add(drop);
//      }
//    }
//    if (CollectionUtils.isNotEmpty(newDrops)) {
//      for (JSONObject drop : newDrops) {
//        executeCommand(drop.toString());
//      }
//    }
//    List<JSONObject> newCreates = new ArrayList<>();
//    List<Document> needDrops = new ArrayList<>();
//
//    for (JSONObject create : creates) {
//      String collectionName = (String) create.get("createIndexes");
//      ListIndexesIterable<Document> documents = mongoTemplate.getCollection(collectionName).listIndexes();
//      Map<String, MongoIndex> existIndexMap = new HashMap<>();
//      for (Document document : documents) {
//        existIndexMap.put((String) document.get("name"), JSONUtil.toBean(JSONUtil.toJsonStr(document), MongoIndex.class));
//      }
//
//      JSONArray jsonArray = (JSONArray) create.get("indexes");
//
//      if (CollectionUtils.isEmpty(jsonArray)) {
//        continue;
//      }
//      JSONArray newJsonArray = new JSONArray();
//      for (Object index : jsonArray) {
//        JSONObject jsonObject = (JSONObject) index;
//        MongoIndex mongoIndex = jsonObject.toBean(MongoIndex.class);
//        //查询索引是否存在，不存在不能删除
//        MongoIndex mongoIndex1 = existIndexMap.get(mongoIndex.getName());
//        if (!mongoIndex.equals(mongoIndex1)) {
//          if (mongoIndex1 != null && mongoIndex.getName().equals(mongoIndex1.getName())) {
//            Document drop = new Document();
//            drop.put("dropIndexes", collectionName);
//            drop.put("index", mongoIndex.getName());
//            needDrops.add(drop);
//
//          }
//          newJsonArray.add(index);
//        }
//      }
//
//      if (newJsonArray.size() != 0) {
//        create.set("indexes", newJsonArray);
//        newCreates.add(create);
//      }
//    }
//
//    if (CollectionUtils.isNotEmpty(needDrops)) {
//      for (Document drop : needDrops) {
//        executeCommand(drop.toJson());
//      }
//    }
//
//    if (CollectionUtils.isNotEmpty(newCreates)) {
//      for (JSONObject create : newCreates) {
//        executeCommand(create.toString());
//      }
//    }
//  }
//
//
//  public void executeCommand(String scripts) {
//    try {
//      Document document = mongoTemplate.executeCommand(scripts);
//    } catch (Exception e) {
//      log.warn("execute scripts failed, scripts = "+ scripts, e);
//    }
//
//  }
//
//}
