/**
 * @title: DeletePermissionRoleMappingDto
 * @description:
 * @author lk
 * @date 2021/12/6
 */
package com.tapdata.tm.user.dto;

import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
public class UpdatePermissionRoleMappingDto {

	private List<RoleMappingDto> adds;

	private List<RoleMappingDto> deletes;

}
