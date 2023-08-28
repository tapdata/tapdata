package com.tapdata.tm.ds.vo;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/28 19:47 Create
 */
@Data
public class DsGroupTypeTagsVo implements Serializable {
	String _id;
	List<String> tags;
}
