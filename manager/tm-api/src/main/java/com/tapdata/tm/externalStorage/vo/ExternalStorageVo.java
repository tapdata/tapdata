package com.tapdata.tm.externalStorage.vo;

import lombok.Data;

/**
 * @author samuel
 * @Description
 * @create 2023-02-09 14:29
 **/
@Data
public class ExternalStorageVo {
	private String name;
	private String type;
	private String uri;
	private String table;
}