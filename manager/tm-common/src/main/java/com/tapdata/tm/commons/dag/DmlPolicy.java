package com.tapdata.tm.commons.dag;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

/**
 * @author samuel
 * @Description
 * @create 2022-06-27 17:55
 **/
@ToString
@Getter
@Setter
@NoArgsConstructor
public class DmlPolicy implements Serializable {
	@JsonIgnore
	private static final long serialVersionUID = -1095574969265783430L;
	private Boolean insertEvent;
	private Boolean updateEvent;
	private Boolean deleteEvent;
	private DmlPolicyEnum insertPolicy;
	private DmlPolicyEnum updatePolicy;
}
