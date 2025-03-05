package com.tapdata.tm.init;

import com.mongodb.client.model.Collation;
import lombok.Data;
import org.bson.Document;

@Data
public class MongoIndex {
    private boolean background;
    private boolean unique;
    private String name;
    private boolean sparse;
    private Long expireAfterSeconds;
    private Integer version;
    private Document weights;
    private String defaultLanguage;
    private String languageOverride;
    private Integer textVersion;
    private Integer sphereVersion;
    private Integer bits;
    private Double min;
    private Double max;
    private Double bucketSize;
    private Document storageEngine;
    private Document partialFilterExpression;
    private Collation collation;
    private Document wildcardProjection;
    private boolean hidden;
    private Document key;
    private Integer v;
}
