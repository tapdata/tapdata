package com.tapdata.tm.task.service.batchin;


import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.task.service.batchin.constant.KeyWords;
import com.tapdata.tm.task.service.batchin.entity.ParseParam;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class ParseUnKnowVersionRelMigFileTest {
    @Test
    void testUnSupport() {
        ParseParam param = new ParseParam();
        Map<String, Object> info = new HashMap<>();
        info.put(KeyWords.VERSION, "1.3.1");
        param.setRelMigInfo(info);
        ParseUnKnowVersionRelMigFile parseUnKnowVersionRelMigFile = new ParseUnKnowVersionRelMigFile(param);
        Assertions.assertThrows(BizException.class, () -> {
            try {
                parseUnKnowVersionRelMigFile.parse();
            } catch (BizException e) {
                Assertions.assertEquals("relMig.parse.unSupport", e.getErrorCode());
                throw e;
            }
        });
    }
}