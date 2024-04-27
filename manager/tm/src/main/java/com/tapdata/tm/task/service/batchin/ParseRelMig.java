package com.tapdata.tm.task.service.batchin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.task.service.batchin.constant.KeyWords;
import com.tapdata.tm.task.service.batchin.constant.ParseRelMigFileVersionMapping;
import com.tapdata.tm.task.service.batchin.dto.RelMigBaseDto;
import com.tapdata.tm.task.service.batchin.entity.ParseParam;
import com.tapdata.tm.task.service.batchin.handle.ParseRelMigToDto;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface ParseRelMig<T, Dto extends RelMigBaseDto>  {
    String PROCESSOR_THREAD_NUM = "processorThreadNum";
    String CATALOG = "catalog";
    String ELEMENT_TYPE = "elementType";
    String PROCESSOR = "processor";
    String RM_ID_KEY = "rm_id";

    List<T> parse();

    static ParseRelMigFile redirect(ParseParam<RelMigBaseDto> param) throws IOException {
        MultipartFile multipartFile = param.getMultipartFile();
        String relMig = new String(multipartFile.getBytes());
        RelMigBaseDto migBaseDto = ParseRelMigToDto.parseToBaseDto(relMig);
        param.setRelMig(migBaseDto);
        param.setRelMigStr(relMig);
        Map<String, Object> relMigInfo;
        try {
            relMigInfo = new ObjectMapper().readValue(param.getRelMigStr(), HashMap.class);
            param.setRelMigInfo(relMigInfo);
        } catch (Exception e) {
            throw new BizException("Can not convert rmProject");
        }
        try {
            String version = String.valueOf(relMigInfo.get(KeyWords.VERSION));
            Class<? extends ParseRelMig> instance = ParseRelMigFileVersionMapping.getInstance(version);
            Constructor<? extends ParseRelMig> declaredConstructor = instance.getDeclaredConstructor(ParseParam.class);
            return (ParseRelMigFile) declaredConstructor.newInstance(param);
        } catch (Exception e) {
            throw new BizException("Can not convert rmProject");
        }
    }

}
