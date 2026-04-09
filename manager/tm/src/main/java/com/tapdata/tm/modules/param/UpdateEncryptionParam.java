package com.tapdata.tm.modules.param;

import com.tapdata.tm.module.entity.Path;
import lombok.Data;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/20 17:06 Create
 * @description
 */
@Data
public class UpdateEncryptionParam {
    String apiId;
    List<Path> paths;
}
