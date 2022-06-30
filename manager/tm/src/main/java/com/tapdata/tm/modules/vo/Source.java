package com.tapdata.tm.modules.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * database_type: "mongodb"
 * id: "6113498c89443123b98b9636"
 * name: "mongo_changan_car_st"
 * status: "ready"
 */
@AllArgsConstructor
@Data
public class Source {
    @JsonProperty("database_type")
    String databaseType;

    String id;
    String name;
    String status;


}
