package com.tapdata.tm.init;

import com.mongodb.client.model.Collation;
import lombok.Data;
import org.bson.conversions.Bson;

@Data
public class MongoIndex {
    private boolean background;
    private boolean unique;
    private String name;
    private boolean sparse;
    private Long expireAfterSeconds;
    private Integer version;
    private Bson weights;
    private String defaultLanguage;
    private String languageOverride;
    private Integer textVersion;
    private Integer sphereVersion;
    private Integer bits;
    private Double min;
    private Double max;
    private Double bucketSize;
    private Bson storageEngine;
    private Bson partialFilterExpression;
    private Collation collation;
    private Bson wildcardProjection;
    private boolean hidden;
    private Bson key;
}
