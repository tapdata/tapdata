package com.tapdata.tm.permissions.vo;

import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionDataTypeEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import lombok.Data;

import java.io.Serializable;
import java.util.Set;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/8/23 17:24 Create
 */
@Data
public class DataPermissionAuthInfoVo implements Serializable {

	public enum AuthType {
		Un, Admin, UserData, Menu, Role,
	}

	private String id;
	private String userId;
	private DataPermissionMenuEnums menuEnums;
	private DataPermissionDataTypeEnums dataTypeEnums;
	private DataPermissionActionEnums actionEnums;
	private AuthType authType = AuthType.Un;
	private Set<String> actionSet;
	private Set<String> roleSet;
	private Long created;

	public DataPermissionAuthInfoVo id(String id) {this.id = id;return this;}
	public DataPermissionAuthInfoVo userId(String userId) {this.userId = userId;return this;}
	public DataPermissionAuthInfoVo menuEnums(DataPermissionMenuEnums menuEnums) {this.menuEnums = menuEnums;return this;}
	public DataPermissionAuthInfoVo dataTypeEnums(DataPermissionDataTypeEnums dataTypeEnums) {this.dataTypeEnums = dataTypeEnums;return this;}
	public DataPermissionAuthInfoVo actionEnums(DataPermissionActionEnums actionEnums) {this.actionEnums = actionEnums;return this;}
	public DataPermissionAuthInfoVo actionSet(Set<String> actions) {this.actionSet = actions;return this;}
	public DataPermissionAuthInfoVo created(Long created) {this.created = created;return this;}


	public DataPermissionAuthInfoVo authWithAdmin() {this.authType = AuthType.Admin;return this;}
	public DataPermissionAuthInfoVo authWithUserData() {this.authType = AuthType.UserData;return this;}
	public DataPermissionAuthInfoVo authWithMenu() {this.authType = AuthType.Menu;return this;}
	public DataPermissionAuthInfoVo authWithRole() {this.authType = AuthType.Role;return this;}

	public boolean isUnAuth() {
		return AuthType.Un == getAuthType() && !isSetQueryFilter();
	}

	public boolean isSetQueryFilter() {
		return null == id;
	}
}
