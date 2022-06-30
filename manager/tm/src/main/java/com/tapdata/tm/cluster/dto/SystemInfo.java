package com.tapdata.tm.cluster.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/13 下午9:52
 * @description
 */
@AllArgsConstructor
@Getter
@Setter
public class SystemInfo {
    private String hostname;
    private String uuid;
    private String ip;
    private List<String> ips;

    private long time;
    private String accessCode;
    private String username;
    private String process_id;


    private Integer cpus;
    private String os;
    private Long totalmem;
    private String logDir;
    private String work_dir;
    private String installationDirectory;

}
