package com.tapdata.tm.init;

import cn.hutool.core.comparator.VersionComparator;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IORuntimeException;
import cn.hutool.core.io.IoUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONConfig;
import cn.hutool.json.JSONUtil;
import com.tapdata.tm.verison.dto.VersionDto;
import com.tapdata.tm.verison.service.VersionService;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

@Component
@Order(Integer.MIN_VALUE)
@Slf4j
public class ScriptExecutorRunInit implements ApplicationRunner {

  @Autowired
  private MongoTemplate mongoTemplate;

  @Autowired
  private VersionService versionService;

  @Value("#{'${spring.profiles.include:idaas}'.split(',')}")
  private List<String> productList;
  @Override
  public void run(ApplicationArguments args) throws Exception {
//    executeScript("init", VersionDto.SCRIPT_VERSION_KEY);
//    log.info("Execute the initialization script to complete...");

    if (productList.contains("idaas")) {
      executeScript("init/idaas", VersionDto.DAAS_SCRIPT_VERSION_KEY);
      log.info("Execute the daas product initialization script to complete...");
    }
    if (productList.contains("dfs")) {
      executeScript("init/dfs", VersionDto.DFS_SCRIPT_VERSION_KEY);
      log.info("Execute the dfs product initialization script to complete...");
    }
    if (productList.contains("drs")) {
      executeScript("init/drs", VersionDto.DRS_SCRIPT_VERSION_KEY);
      log.info("Execute the drs product initialization script to complete...");
    }

  }

  private void executeScript(String path, String scriptVersionKey) throws IOException {
    PathMatchingResourcePatternResolver pathMatchingResourcePatternResolver = new PathMatchingResourcePatternResolver();
    Resource versionRes = pathMatchingResourcePatternResolver.getResource(ResourceUtils.CLASSPATH_URL_PREFIX + path + "/version");
    if (!versionRes.exists()) {
      throw new RuntimeException("init script file does not exist");
    }

    String version;
    try {
      version = IoUtil.read(versionRes.getInputStream(), StandardCharsets.UTF_8);
    } catch (IORuntimeException | IOException e) {
      throw new RuntimeException("Error reading version file",e);
    } finally {
      IoUtil.close(versionRes.getInputStream());
    }
    log.info("The version of the latest init script is {}", version);


    VersionDto dbVersionDto = versionService.findOne(Query.query(Criteria.where("type").is(scriptVersionKey)));
    VersionComparator versionComparator = new VersionComparator();
    if (dbVersionDto != null && versionComparator.compare(dbVersionDto.getVersion(), version) >= 0) {
      log.info("The data version is up to date and does not need to be updated {}", dbVersionDto.getVersion());
      return;
    }
    String dbVersion = dbVersionDto == null ? null : dbVersionDto.getVersion();
    Resource[] resources = pathMatchingResourcePatternResolver
            .getResources(ResourceUtils.CLASSPATH_URL_PREFIX + path + "/*.json");
    log.info("The number of init script is {}", resources.length);
    Arrays.sort(resources,
            (r1,r2)-> versionComparator.compare(FileUtil.mainName(r1.getFilename()), FileUtil.mainName(r2.getFilename())));

    for (Resource resource : resources) {
      String currentVersion = FileUtil.mainName(resource.getFilename());
      log.info("init script: {} - {}", resource.getFilename(),currentVersion);
      if (versionComparator.compare(currentVersion, dbVersion) <= 0) {
        log.info("The init script has been executed {}, skip...", currentVersion);
        continue;
      }
      String scriptStr;
      try {
        scriptStr = IoUtil.read(resource.getInputStream(), StandardCharsets.UTF_8);
      } catch (IORuntimeException | IOException e) {
        throw new RuntimeException("Error reading script file", e);
      } finally {
        IoUtil.close(resource.getInputStream());
      }
      //todo: If an error occurs, the transaction needs to be rolled back. tm does not currently support it.

      JSONConfig jsonConfig = JSONConfig.create().setOrder(true);
      JSONArray scriptArray = JSONUtil.parseArray(scriptStr, jsonConfig);
      for (Object script : scriptArray) {
        try {
          Document document = mongoTemplate.executeCommand(script.toString());
          log.info("execute script: {} -- {}", script, document.toJson());
        } catch (Exception e) {
          throw new RuntimeException("execute script error: " + script.toString(), e);
        }
      }
      //update db version
      VersionDto versionDto = new VersionDto(scriptVersionKey, currentVersion);
      versionService.upsert(Query.query(Criteria.where("type").is(scriptVersionKey)), versionDto);
      log.info("Update data script to version {} -> {}", dbVersion,  currentVersion);
      dbVersion = currentVersion;

    }
  }

}
