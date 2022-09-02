package io.tapdata.mongodb.entity;

import org.bson.Document;

public interface ToDocument {
	public static final String FIELD_ID = "_id";
	String getId();
	public
	Document toDocument(Document document);
}
