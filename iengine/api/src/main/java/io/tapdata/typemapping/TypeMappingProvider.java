package io.tapdata.typemapping;

import com.tapdata.entity.DbType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author samuel
 * @Description Type mapping interface
 * @create 2021-08-03 17:49
 **/
public interface TypeMappingProvider {

	default List<DbType> bindString() {
		return new ArrayList<>();
	}

	default List<DbType> bindNumber() {
		return new ArrayList<>();
	}

	default List<DbType> bindBytes() {
		return new ArrayList<>();
	}

	default List<DbType> bindBoolean() {
		return new ArrayList<>();
	}

	default List<DbType> bindDate() {
		return new ArrayList<>();
	}

	default List<DbType> bindDatetime() {
		return new ArrayList<>();
	}

	default List<DbType> bindDatetime_with_timezone() {
		return new ArrayList<>();
	}

	default List<DbType> bindTime() {
		return new ArrayList<>();
	}

	default List<DbType> bindTime_with_timezone() {
		return new ArrayList<>();
	}

	default List<DbType> bindArray() {
		return new ArrayList<>();
	}

	default List<DbType> bindMap() {
		return new ArrayList<>();
	}

	default List<DbType> bindNull() {
		return new ArrayList<>();
	}

	default List<DbType> bindUnsupported() {
		return new ArrayList<>();
	}
}
