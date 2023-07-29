package top.qjj.shmily.metadata.bean;

import com.google.gson.JsonElement;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.PrincipalType;

import java.util.List;
import java.util.Map;

/**
 * @author 佳境Shmily
 * @Description: metadata bean
 * @CreateTime: 2023/7/29 13:28
 * @Site: shmily-qjj.top
 */

@Data
@AllArgsConstructor
public class Metadata {
    /**
     * 元数据变更实体类型
     */
    public enum EntityType {
        // 数据库 db
        DATABASE,
        // 表 table
        TABLE,
        // 分区 partition
        PARTITION,
        // 函数 function
        FUNCTION,
        // catalog
        CATALOG,
        // insert
        INSERT,
    }

    private EntityType entityType;
    private String entityFQN;
    private String operation;
    private EntityInfo entityInfo;

    @Data
    @AllArgsConstructor
    public static class EntityInfo {
        private String name;
        private String description;
        private String locationUri;
        private List<FieldSchema> schema;
        private Map<String, String> parameters;
        private String ownerName;
        private PrincipalType ownerType;
        private String catalogName;
        private JsonElement extension;
    }


}
