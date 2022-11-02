package com.tapdata.tm.sdk.auth;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/9 6:37 上午
 * @description
 */
@AllArgsConstructor
@EqualsAndHashCode
@Data
public class BasicCredentials implements Credentials {

	private String accessKey;
	private String accessKeySecret;

	@Override
	public String toString() {
		return "Credential{" +
			"accessKey='" + accessKey + '\'' +
			", accessKeySecret='[PROTECT]'"+
			'}';
	}

}
