package com.tapdata.entity.dataflow.validator;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * 用户设置的校验配置中的总配置
 * Created by xj
 * 2020-04-16 01:52
 **/
@Data
public class ValidatorSettings implements Serializable {

	private static final long serialVersionUID = 645168460852007577L;

	private String id;

	private String validateStatus;

	private String validateFailedMSG;

	private List<ValidatorSetting> validateSettings;
}
