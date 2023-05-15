package com.tapdata.tm.cluster.dto;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * {
 *     "downloadUrl": "http://resource.tapdata.net/package/feagent/dfs-v2.0.0-111001/",
 *     "process_id": "61777c0a7a2ab872adc4dd3c-1fitbp2h6",
 *     "version": "v2.0.0-111001",
 *     "token": "a/HZzXh5MDbwPGd8hCzZYYF0XXgDZ287oY34Sx3QAq5Z7zikkMRcI62kZHXq8RRJj6VrJcSY6ehw4iM8d8LW1fyva0LytL1MqHQaeaULvnh8xPHZN0uE/bMQaLsVhgVQ8e2eYfTXkziZ/wGJItF0yKQt0PekI1YjlHYkRUt9Na6MP0Rr+tEXXFIszkUJ3JOvTKOLd2UIFv2G6JUuX3jN1piR6bLX6zo7Tu8UdiTr+QIYxW7BrF/iigVRK2Vvu/fqjd81LoOElHpbk9IjrFCpuXIsdoAofn/PZ2yF1oO64Ho/y+IR5iJWPOWXznTFqIXF9THkVT0jgwuzV8s6OPNp+X7QpooUEKWShHdZwMEELPPtOVQ877J7omQiwDdRE0+dScB26sSh+6zjcozZ2G+Q4wd6GkEHeeB1+bef8ef218tKEp0XBbIGjkngFkGTaFmwASAFyGCsUO8wz7NoloPaVwwAtXXAUYJCegnVsS6+9QeC3jdqaTX2hYeqk2UAGAt3uahwafnsl4J05m+ruiSlZ/fZPA/0RjDMhE4ETxvFOwylrvrURDdtUfkE//wzEG4L6E6o/DUYSVqFAhXsIzCW+hU51+EtB4PeFPmQC7CPhyic0kk7VQ8PGTw+D1cbxQuC"
 * }
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class UpdateAgentVersionParam {
    private String downloadUrl;

    @JsonProperty("process_id")
    private String processId;
    private String version;
    private String token;

    /**
     * upgrade: 升级实例版本
     * start: 启动agent
     * restart: 重启agent
     */
    private String op;

}
