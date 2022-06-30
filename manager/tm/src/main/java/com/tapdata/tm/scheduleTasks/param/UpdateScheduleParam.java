package com.tapdata.tm.scheduleTasks.param;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Map;

@Data
public class UpdateScheduleParam {

    @JsonProperty("$set")
    private Map set;
}
