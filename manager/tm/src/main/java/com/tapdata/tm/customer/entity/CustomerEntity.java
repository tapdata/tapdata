package com.tapdata.tm.customer.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2022/2/14 下午1:04
 */
@Document("Customer")
@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@ToString
public class CustomerEntity extends BaseEntity {

    private String companyName; //: "", // 企业名称，必填
    private String website; //: "",    // 公司网站
    private String industry; //: "",    // 所属行业
    private String country; //： "",    // 国家
    private String province; //: "",    // 省份
    private String city; //: "",        // 城市

    private String phone; //: "",       // 联系电话
    private String contact; //: "",     // 联系人

}
