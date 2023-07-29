package top.qjj.shmily.lineage.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * @author 佳境Shmily
 * @Description: 血缘消息 发给kafka服务或rest接口
 * @CreateTime: 2023/7/29 11:52
 * @Site: shmily-qjj.top
 */

@Data
@AllArgsConstructor
public class LineageMsg implements Serializable {
    private String topic;
    private String body;
}
