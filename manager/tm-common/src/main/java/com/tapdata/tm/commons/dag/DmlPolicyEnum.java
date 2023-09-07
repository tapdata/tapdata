package com.tapdata.tm.commons.dag;

/**
 * @author samuel
 * @Description DML处理策略枚举类
 * @create 2022-06-27 17:41
 **/
public enum DmlPolicyEnum {
	update_on_exists,
	ignore_on_exists,
	ignore_on_nonexists,
	insert_on_nonexists,
	log_on_nonexists
}
