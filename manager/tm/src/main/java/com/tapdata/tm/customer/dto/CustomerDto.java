package com.tapdata.tm.customer.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.validation.constraints.NotEmpty;


/**
 * Customer
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class CustomerDto extends BaseDto {

    @NotEmpty
    private String companyName; //: "", // 企业名称，必填
    private String website; //: "",    // 公司网站
    private String industry; //: "",    // 所属行业
    private String country; //： "",    // 国家
    private String province; //: "",    // 省份
    private String city; //: "",        // 城市

    private String phone; //: "",       // 联系电话
    private String contact; //: "",     // 联系人

}
