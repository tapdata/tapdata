/**
 * @title: DeletePermissionRoleMappingDto
 * @description:
 * @author lk
 * @date 2021/12/6
 */
package com.tapdata.tm.user.dto;

import com.tapdata.tm.roleMapping.dto.RoleMappingDto;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class DeletePermissionRoleMappingDto {

	private List<RoleMappingDto> data;

}
