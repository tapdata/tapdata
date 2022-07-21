package com.tapdata.entity;

/**
 * @author samuel
 * @Description
 * @create 2021-08-19 14:17
 **/
public enum MongoUpdateType {
	SET("$set"),
	UNSET("$unset"),
	ADD_TO_SET("$addToSet"),
	PULL("$pull"),
	;

	private final String keyWord;

	MongoUpdateType(String keyWord) {
		this.keyWord = keyWord;
	}

	public String getKeyWord() {
		return keyWord;
	}
}
