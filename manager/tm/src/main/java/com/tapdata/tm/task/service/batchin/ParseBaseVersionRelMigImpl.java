package com.tapdata.tm.task.service.batchin;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.task.service.batchin.entity.ParseParam;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * this instance will support parse relMig file when version value is 1.2.0
 * @author Gavin'Xiao
 * */
public class ParseBaseVersionRelMigImpl extends ParseRelMigFile {

    public ParseBaseVersionRelMigImpl(ParseParam param) {
        super(param);
    }

    @Override
    public List<TaskDto> parse() {
        try {
            Map<String, String> tasks = doParse(param.getSource(), param.getSink(), param.getUser());
            if (tasks == null) {
                return new ArrayList<>();
            }
            List<TaskDto> tpTasks = new ArrayList<>();
            for (String key : tasks.keySet()) {
                TaskDto tpTask = JsonUtil.parseJsonUseJackson(tasks.get(key), TaskDto.class);
                tpTask.setTransformTaskId(new ObjectId().toHexString());
                tpTasks.add(tpTask);
            }
            return tpTasks;
        } catch (Exception e) {
            throw new BizException("relMig.parse.failed", e.getMessage());
        }
    }
}
