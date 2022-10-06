package io.tapdata.mongodb.entity;

import org.bson.Document;

public interface ToDocument {
	String FIELD_ID = "_id";
	Object getId();
	public
	Document toDocument(Document document);
}
