package com.tapdata.tm.task.service.batchin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.task.service.batchin.constant.KeyWords;
import com.tapdata.tm.task.service.batchin.constant.ParseRelMigFileVersionMapping;
import com.tapdata.tm.task.service.batchin.entity.ParseParam;
import org.springframework.web.multipart.MultipartFile;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

public interface ParseRelMig<T>  {
    String PROCESSOR_THREAD_NUM = "processorThreadNum";
    String CATALOG = "catalog";
    String ELEMENT_TYPE = "elementType";
    String PROCESSOR = "processor";
    String RM_ID = "rm_id";

    List<T> parse();

    static ParseRelMigFile redirect(ParseParam param) {
        try {
            MultipartFile multipartFile = param.getMultipartFile();
            String relMig = new String(multipartFile.getBytes());
            param.setRelMigStr(relMig);
            Map<String, Object> relMigInfo;
            relMigInfo = new ObjectMapper().readValue(param.getRelMigStr(), Map.class);
            param.setRelMigInfo(relMigInfo);
            String version = String.valueOf(relMigInfo.get(KeyWords.VERSION));
            Class<? extends ParseRelMig> instance = ParseRelMigFileVersionMapping.getInstance(version);
            Constructor<? extends ParseRelMig> declaredConstructor = instance.getDeclaredConstructor(ParseParam.class);
            return (ParseRelMigFile) declaredConstructor.newInstance(param);
        } catch (Exception e) {
            throw new BizException("relMig.parse.failed", e.getMessage());
        }
    }

}
