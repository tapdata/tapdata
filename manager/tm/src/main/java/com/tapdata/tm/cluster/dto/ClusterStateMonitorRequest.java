/**
 * @title: EditMonitorRequest
 * @description:
 * @author lk
 * @date 2021/12/7
 */
package com.tapdata.tm.cluster.dto;

import javax.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ClusterStateMonitorRequest {

	@NotBlank(groups = {ValidationType.EditMonitor.class, ValidationType.RemoveMonitor.class})
	private String id;

	private String arguments;

	@NotBlank(groups = {ValidationType.AddMonitor.class, ValidationType.EditMonitor.class})
	private String command;

	@NotBlank(groups = {ValidationType.AddMonitor.class, ValidationType.EditMonitor.class})
	private String name;

	@NotBlank(groups = {ValidationType.AddMonitor.class, ValidationType.EditMonitor.class, ValidationType.RemoveMonitor.class})
	private String uuid;


	public interface ValidationType {

		interface AddMonitor {}
		interface EditMonitor {}
		interface RemoveMonitor {}
	}
}
