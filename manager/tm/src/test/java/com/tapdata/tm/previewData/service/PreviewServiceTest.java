package com.tapdata.tm.previewData.service;

import com.tapdata.tm.BaseJunit;
import com.tapdata.tm.previewData.param.PreviewParam;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PreviewServiceTest extends BaseJunit {

    @Autowired
    PreviewService previewService;

    @Test
    void preview() {
        PreviewParam previewParam=new PreviewParam();
        previewParam.setId("61c298044dc2856447bb53d2");
        previewParam.setLimit(10L);
        previewParam.setSkip(1);
        Map result= previewService.preview(previewParam);
        printResult(result);
    }
}