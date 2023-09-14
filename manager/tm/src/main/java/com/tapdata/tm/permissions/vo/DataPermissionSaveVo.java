package com.tapdata.tm.permissions.vo;

import com.tapdata.tm.commons.base.DataPermissionAction;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/15 17:18 Create
 */
@Data
public class DataPermissionSaveVo implements Serializable {
	private String dataType;
	private String dataId;
	private List<DataPermissionAction> actions;
}
