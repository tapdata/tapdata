package com.tapdata.entity;

/**
 * @author samuel
 * @Description Type mapping direction
 * @create 2021-08-09 15:52
 **/
public enum TypeMappingDirection {
	ALL, // Two-way mapping
	TO_TAPTYPE, // Can only be used for dbType->tapType mapping
	TO_DATATYPE, // Can only be used for tapType->dbType mapping
	;
}
