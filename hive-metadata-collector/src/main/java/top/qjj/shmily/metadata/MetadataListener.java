package top.qjj.shmily.metadata;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.events.*;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.ql.exec.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.qjj.shmily.metadata.bean.Metadata;
import top.qjj.shmily.metadata.utils.HttpClient;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

/**
 * @author 佳境Shmily
 * @Description: HiveMetaStore监听器 实时收集Hive元数据变更
 * @CreateTime: 2023/7/29 13:35
 * @Site: shmily-qjj.top
 * @References: Hive源码 org.apache.hive.hcatalog.listener.DbNotificationListener or org.apache.hive.hcatalog.listener.NotificationListener
 * @Usage: [hive-site.xml] hive.metastore.event.listeners=top.qjj.shmily.metadata.MetadataListener
 */
public class MetadataListener extends MetaStoreEventListener {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataListener.class);
    private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private static final JsonParser jsonParser = new JsonParser();
    private final static String META_SERVER_REST = "http://192.168.82.30:8080/json";

    public MetadataListener(final Configuration config) {
        super(config);
    }

    // 1. Database相关元数据变更

    /**
     * 创建数据库 listener
     * eg: {"entityType":"DATABASE","entityFQN":"hive.test","operation":"CREATE_DATABASE","entityInfo":{"name":"test","description":"test_db","locationUri":"hdfs://shmily:8020/user/hive/warehouse/test.db","ownerName":"hdfs","ownerType":"USER","catalogName":"hive","extension":{}}}
     * @param dbEvent CreateDatabaseEvent
     */
    @Override
    public void onCreateDatabase(CreateDatabaseEvent dbEvent) {
        if (dbEvent.getStatus()) {
            try {
                Database database = dbEvent.getDatabase();
                String catalogName = database.getCatalogName();
                if (catalogName == null || "".equals(catalogName)) {
                    catalogName = DEFAULT_CATALOG_NAME;
                }
                StringBuilderWriter ext = new StringBuilderWriter();
                JsonWriter writer = new JsonWriter(ext);
                try {
                    writer.beginObject();
                    if (database.getParametersSize() != 0) {
                        writer.name("parameters").value(database.getParameters().toString());
                    }
                    writer.endObject();
                    writer.close();
                } catch (IOException e) {
                    LOG.warn(e.toString());
                }
                Metadata.EntityInfo entityInfo = new Metadata.EntityInfo(
                        database.getName(),
                        database.getDescription(),
                        database.getLocationUri(),
                        null,
                        database.getParameters(),
                        database.getOwnerName(),
                        database.getOwnerType(),
                        catalogName,
                        jsonParser.parse(ext.toString())
                );

                Metadata metadata = new Metadata(Metadata.EntityType.DATABASE,
                        getEntityFQN(entityInfo),
                        EventMessage.EventType.CREATE_DATABASE.toString(),
                        System.currentTimeMillis(),
                        entityInfo);
                send(metadata);
            } catch (Exception e) {
                LOG.warn("Failed to get metadata, query was not affected. errMsg: {}", e.getMessage());
            }

        }
    }

    /**
     * 删库跑路 listener
     * eg: {"entityType":"DATABASE","entityFQN":"hive.test","operation":"DROP_DATABASE","entityInfo":{"name":"test","locationUri":"hdfs://shmily:8020/user/hive/warehouse/test.db","parameters":{},"ownerName":"hdfs","ownerType":"USER","catalogName":"hive","extension":{}}}
     * note: drop database test cascade; 如果test库下面存在表，强制删除时会触发多条DROP_TABLE记录以及一条DROP_DATABASE记录
     * @param dbEvent DropDatabaseEvent
     */
    @Override
    public void onDropDatabase(DropDatabaseEvent dbEvent) {
        if (dbEvent.getStatus()) {
            try {
                Database database = dbEvent.getDatabase();
                String catalogName = database.getCatalogName();
                if (catalogName == null || "".equals(catalogName)) {
                    catalogName = DEFAULT_CATALOG_NAME;
                }
                StringBuilderWriter ext = new StringBuilderWriter();
                JsonWriter writer = new JsonWriter(ext);
                try {
                    writer.beginObject();
                    if (database.getParametersSize() != 0) {
                        writer.name("parameters").value(database.getParameters().toString());
                    }
                    writer.endObject();
                    writer.close();
                } catch (IOException e) {
                    LOG.warn(e.toString());
                }

                Metadata.EntityInfo entityInfo = new Metadata.EntityInfo(
                        database.getName(),
                        database.getDescription(),
                        database.getLocationUri(),
                        null,
                        database.getParameters(),
                        database.getOwnerName(),
                        database.getOwnerType(),
                        catalogName,
                        jsonParser.parse(ext.toString())
                );

                Metadata metadata = new Metadata(Metadata.EntityType.DATABASE,
                        getEntityFQN(entityInfo),
                        EventMessage.EventType.DROP_DATABASE.toString(),
                        System.currentTimeMillis(),
                        entityInfo);
                send(metadata);
            }catch (Exception e) {
                LOG.warn("Failed to get metadata, query was not affected. errMsg: {}", e.getMessage());
            }

        }
    }

    // 2. Table相关元数据变更

    /**
     * 建表 listener
     * eg: {"entityType":"TABLE","entityFQN":"hive.default.test_part","operation":"CREATE_TABLE","entityInfo":{"name":"default.test_part","locationUri":"hdfs://shmily:8020/user/hive/warehouse/test_part","schema":[{"name":"id","type":"int"},{"name":"name","type":"string"}],"parameters":{"transient_lastDdlTime":"1690795500","bucketing_version":"2"},"ownerName":"hdfs","ownerType":"USER","catalogName":"hive","extension":{"createTime":1690795500,"lastAccessTime":0,"isPartitioned":true,"partitionKeys":"ds#string","skewedInfo":"SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{})","inputFormat":"org.apache.hadoop.mapred.TextInputFormat","outputFormat":"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat","serializationLib":"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"}}}
     * @param tableEvent CreateTableEvent
     */
    @Override
    public void onCreateTable(CreateTableEvent tableEvent) {
        if(tableEvent.getStatus()) {
            try {
                Table t = tableEvent.getTable();
                StringBuilderWriter ext = new StringBuilderWriter();
                JsonWriter writer = new JsonWriter(ext);
                try {
                    writer.beginObject();
                    if (t.getCreationMetadata() != null) {
                        writer.name("creationMetadata").value(t.getCreationMetadata().toString());
                    }
                    writer.name("createTime").value(t.getCreateTime());
                    writer.name("lastAccessTime").value(t.getLastAccessTime());
                    if (t.isSetPartitionKeys()){
                        writer.name("isPartitioned").value(true);
                        List<String> partitionKeys = t.getPartitionKeys().stream().map(x -> String.format("%s#%s", x.getName(), x.getType())).collect(Collectors.toList());
                        writer.name("partitionKeys").value(StringUtils.join(partitionKeys, ","));
                    }else {
                        writer.name("isPartitioned").value(false);
                    }
                    if (t.getSd().getBucketColsSize() != 0) {
                        writer.name("bucketCols").value(StringUtils.join(t.getSd().getBucketCols(), ","));
                    }
                    if (t.getSd().getSkewedInfo() != null) {
                        writer.name("skewedInfo").value(t.getSd().getSkewedInfo().toString());
                    }
//                    if (t.getPrivileges() != null) {
//                        writer.name("privileges").value(t.getPrivileges().toString());
//                    }
                    writer.name("inputFormat").value(t.getSd().getInputFormat());
                    writer.name("outputFormat").value(t.getSd().getOutputFormat());
                    writer.name("serializationLib").value(t.getSd().getSerdeInfo().getSerializationLib());
                    writer.endObject();
                    writer.close();
                } catch (IOException e) {
                    LOG.warn(e.toString());
                }

                Metadata.EntityInfo entityInfo = new Metadata.EntityInfo(
                        String.format("%s.%s", t.getDbName(), t.getTableName()),
                        t.getSd().getSerdeInfo().getDescription(),
                        t.getSd().getLocation(),
                        t.getSd().getCols(),
                        t.getParameters(),
                        t.getOwner(),
                        t.getOwnerType(),
                        t.isSetCatName() ? t.getCatName() : DEFAULT_CATALOG_NAME,
                        jsonParser.parse(ext.toString())
                );

                Metadata metadata = new Metadata(Metadata.EntityType.TABLE,
                        getEntityFQN(entityInfo),
                        EventMessage.EventType.CREATE_TABLE.toString(),
                        System.currentTimeMillis(),
                        entityInfo);
                send(metadata);
            } catch (Exception e) {
                LOG.warn("Failed to get metadata, query was not affected. errMsg: {}", e.getMessage());
            }
        }

    }

    /**
     * 删表 listener
     * eg: {"entityType":"TABLE","entityFQN":"hive.default.test_part","operation":"DROP_TABLE","entityInfo":{"name":"default.test_part","locationUri":"hdfs://shmily:8020/user/hive/warehouse/test_part","schema":[{"name":"id","type":"int"},{"name":"name","type":"string"}],"parameters":{"transient_lastDdlTime":"1690795135","bucketing_version":"2"},"ownerName":"hdfs","ownerType":"USER","catalogName":"hive","extension":{}}}
     * @param tableEvent table event.
     */
    @Override
    public void onDropTable(DropTableEvent tableEvent) {
        if(tableEvent.getStatus()) {
            try {
                Table t = tableEvent.getTable();
                Metadata.EntityInfo entityInfo = new Metadata.EntityInfo(
                        String.format("%s.%s", t.getDbName(), t.getTableName()),
                        t.getSd().getSerdeInfo().getDescription(),
                        t.getSd().getLocation(),
                        t.getSd().getCols(),
                        t.getParameters(),
                        t.getOwner(),
                        t.getOwnerType(),
                        t.isSetCatName() ? t.getCatName() : DEFAULT_CATALOG_NAME,
                        jsonParser.parse("{}")
                );

                Metadata metadata = new Metadata(Metadata.EntityType.TABLE,
                        getEntityFQN(entityInfo),
                        EventMessage.EventType.DROP_TABLE.toString(),
                        System.currentTimeMillis(),
                        entityInfo);
                send(metadata);
            } catch (Exception e) {
                LOG.warn("Failed to get metadata, query was not affected. errMsg: {}", e.getMessage());
            }
        }
    }



    /**
     * alter table listener
     * eg: {"entityType":"TABLE","entityFQN":"hive.default.test_part","operation":"ALTER_TABLE","entityInfo":{"name":"default.test_part","locationUri":"hdfs://shmily:8020/user/hive/warehouse/test_part","schema":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"new_col1","type":"string","comment":"新字段呀哈哈"},{"name":"new_col2","type":"int"}],"parameters":{"transient_lastDdlTime":"1690795826","bucketing_version":"2","last_modified_time":"1690795826","last_modified_by":"hdfs"},"ownerName":"hdfs","ownerType":"USER","catalogName":"hive","extension":{"createTime":1690795500,"lastAccessTime":0,"partitionKeys":"ds","skewedInfo":"SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{})","inputFormat":"org.apache.hadoop.mapred.TextInputFormat","outputFormat":"org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat","serializationLib":"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"}}}
     * * @param tableEvent alter table event
     */
    @Override
    public void onAlterTable(AlterTableEvent tableEvent) {
        if (tableEvent.getStatus()) {
            try {
                // alter后的表信息收集
                Table t = tableEvent.getNewTable();
                StringBuilderWriter ext = new StringBuilderWriter();
                JsonWriter writer = new JsonWriter(ext);
                try {
                    writer.beginObject();
                    if (t.getCreationMetadata() != null) {
                        writer.name("creationMetadata").value(t.getCreationMetadata().toString());
                    }
                    writer.name("createTime").value(t.getCreateTime());
                    writer.name("lastAccessTime").value(t.getLastAccessTime());
                    if (t.getPartitionKeysSize() != 0){
                        List<String> partitionKeys = t.getPartitionKeys().stream().map(FieldSchema::getName).collect(Collectors.toList());
                        writer.name("partitionKeys").value(StringUtils.join(partitionKeys, ","));
                    }
                    if (t.getSd().getBucketColsSize() != 0) {
                        writer.name("bucketCols").value(StringUtils.join(t.getSd().getBucketCols(), ","));
                    }
                    if (t.getSd().getSkewedInfo() != null) {
                        writer.name("skewedInfo").value(t.getSd().getSkewedInfo().toString());
                    }
//                    if (t.getPrivileges() != null) {
//                        writer.name("privileges").value(t.getPrivileges().toString());
//                    }
                    writer.name("inputFormat").value(t.getSd().getInputFormat());
                    writer.name("outputFormat").value(t.getSd().getOutputFormat());
                    writer.name("serializationLib").value(t.getSd().getSerdeInfo().getSerializationLib());
                    writer.endObject();
                    writer.close();
                } catch (IOException e) {
                    LOG.warn(e.toString());
                }

                Metadata.EntityInfo entityInfo = new Metadata.EntityInfo(
                        String.format("%s.%s", t.getDbName(), t.getTableName()),
                        t.getSd().getSerdeInfo().getDescription(),
                        t.getSd().getLocation(),
                        t.getSd().getCols(),
                        t.getParameters(),
                        t.getOwner(),
                        t.getOwnerType(),
                        t.isSetCatName() ? t.getCatName() : DEFAULT_CATALOG_NAME,
                        jsonParser.parse(ext.toString())
                );

                Metadata metadata = new Metadata(Metadata.EntityType.TABLE,
                        getEntityFQN(entityInfo),
                        EventMessage.EventType.ALTER_TABLE.toString(),
                        System.currentTimeMillis(),
                        entityInfo);
                send(metadata);
            } catch (Exception e) {
                LOG.warn("Failed to get metadata, query was not affected. errMsg: {}", e.getMessage());
            }
        }
    }


    // 3. Partition相关元数据变更

    /**
     * add partition listener
     * eg: {"entityType":"PARTITION","entityFQN":"hive.default.test_part.ds","operation":"ADD_PARTITION","entityInfo":{"name":"default.test_part.ds","description":"PartitionColumn","locationUri":"hdfs://shmily:8020/user/hive/warehouse/test_part","schema":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"new_col1","type":"string","comment":"??????"},{"name":"new_col2","type":"int"}],"parameters":{"last_modified_time":"1690795826","transient_lastDdlTime":"1690795826","bucketing_version":"2","last_modified_by":"hdfs"},"ownerName":"hdfs","ownerType":"USER","catalogName":"hive","extension":{"ds":"20230731"}}}
     * @param partitionEvent partition event
     */
    @Override
    public void onAddPartition(AddPartitionEvent partitionEvent) {
        if (partitionEvent.getStatus()) {
            try {
                Table table = partitionEvent.getTable();
                Iterator<Partition> partitionIterator = partitionEvent.getPartitionIterator();
                Map<String, List<String>> partitionKeysValues = getPartitionKeysValues(table, partitionIterator);
                for (String partitionKey: partitionKeysValues.keySet()) {
                    StringBuilderWriter addedPartitions = new StringBuilderWriter();
                    JsonWriter writer = new JsonWriter(addedPartitions);
                    writer.beginObject();
                    partitionKeysValues.get(partitionKey).forEach(pv -> {
                        try {
                            writer.name(partitionKey).value(pv);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    writer.endObject();
                    writer.close();

                    Metadata.EntityInfo entityInfo = new Metadata.EntityInfo(
                            String.format("%s.%s.%s", table.getDbName(), table.getTableName(), partitionKey),
                            table.getSd().getCols().stream().filter(c -> Objects.equals(c.getName(), partitionKey))
                                    .map(FieldSchema::getComment).reduce((x, y) -> String.format("%s.%s", x, y))
                                    .orElse("PartitionColumn"),
                            table.getSd().getLocation(),
                            table.getSd().getCols(),
                            table.getParameters(),
                            table.getOwner(),
                            table.getOwnerType(),
                            table.isSetCatName() ? table.getCatName() : DEFAULT_CATALOG_NAME,
                            jsonParser.parse(addedPartitions.toString())
                    );
                    Metadata metadata = new Metadata(Metadata.EntityType.PARTITION,
                            getEntityFQN(entityInfo),
                            EventMessage.EventType.ADD_PARTITION.toString(),
                            System.currentTimeMillis(),
                            entityInfo);
                    send(metadata);
                }
            }catch (Exception e) {
                LOG.warn("Failed to get metadata, query was not affected. errMsg: {}", e.getMessage());
            }
        }
    }


    /**
     * 删除分区 listener
     * eg: {"entityType":"PARTITION","entityFQN":"hive.default.test_part.ds","operation":"DROP_PARTITION","entityInfo":{"name":"default.test_part.ds","description":"PartitionColumn","locationUri":"hdfs://shmily:8020/user/hive/warehouse/test_part","schema":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"new_col1","type":"string","comment":"??????"},{"name":"new_col2","type":"int"}],"parameters":{"last_modified_time":"1690795826","transient_lastDdlTime":"1690795826","bucketing_version":"2","last_modified_by":"hdfs"},"ownerName":"hdfs","ownerType":"USER","catalogName":"hive","extension":{"ds":"20230729"}}}
     * @param partitionEvent partition event
     */
    @Override
    public void onDropPartition(DropPartitionEvent partitionEvent) {
        if (partitionEvent.getStatus()) {
            try {
                Table table = partitionEvent.getTable();
                Iterator<Partition> partitionIterator = partitionEvent.getPartitionIterator();
                Map<String, List<String>> partitionKeysValues = getPartitionKeysValues(table, partitionIterator);
                for (String partitionKey: partitionKeysValues.keySet()) {
                    StringBuilderWriter deletedPartition = new StringBuilderWriter();
                    JsonWriter writer = new JsonWriter(deletedPartition);
                    writer.beginObject();
                    partitionKeysValues.get(partitionKey).forEach(pv -> {
                        try {
                            writer.name(partitionKey).value(pv);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    writer.endObject();
                    writer.close();

                    Metadata.EntityInfo entityInfo = new Metadata.EntityInfo(
                            String.format("%s.%s.%s", table.getDbName(), table.getTableName(), partitionKey),
                            table.getSd().getCols().stream().filter(c -> Objects.equals(c.getName(), partitionKey))
                                    .map(FieldSchema::getComment).reduce((x, y) -> String.format("%s.%s", x, y))
                                    .orElse("PartitionColumn"),
                            table.getSd().getLocation(),
                            table.getSd().getCols(),
                            table.getParameters(),
                            table.getOwner(),
                            table.getOwnerType(),
                            table.isSetCatName() ? table.getCatName() : DEFAULT_CATALOG_NAME,
                            jsonParser.parse(deletedPartition.toString())
                    );
                    Metadata metadata = new Metadata(Metadata.EntityType.PARTITION,
                            getEntityFQN(entityInfo),
                            EventMessage.EventType.DROP_PARTITION.toString(),
                            System.currentTimeMillis(),
                            entityInfo);
                    send(metadata);
                }
            }catch (Exception e) {
                LOG.warn("Failed to get metadata, query was not affected. errMsg: {}", e.getMessage());
            }
        }
    }

    /**
     * 修改分区 listener
     * alter table test_part partition (ds='20230729') rename to partition (ds='20230728');
     * alter table test_part partition (ds='20230730') SET LOCATION 'hdfs://shmily:8020/user/hive/warehouse/test_part/ds=20230730';
     * insert into ...
     * eg: {"entityType":"PARTITION","entityFQN":"hive.default.test_part.ds","operation":"ALTER_PARTITION","entityInfo":{"name":"default.test_part.ds","description":"PartitionColumn","locationUri":"hdfs://shmily:8020/user/hive/warehouse/test_part","schema":[{"name":"id","type":"int"},{"name":"name","type":"string"},{"name":"new_col1","type":"string","comment":"??????"},{"name":"new_col2","type":"int"}],"parameters":{"last_modified_time":"1690795826","transient_lastDdlTime":"1690795826","bucketing_version":"2","last_modified_by":"hdfs"},"ownerName":"hdfs","ownerType":"USER","catalogName":"hive","extension":{"ds":"20230728"}}}
     * @param partitionEvent partition event
     */
    @Override
    public void onAlterPartition(AlterPartitionEvent partitionEvent) {
        if (partitionEvent.getStatus()) {
            try {
                Table table = partitionEvent.getTable();
                Partition newPartition = partitionEvent.getNewPartition();
                Map<String, List<String>> partitionKeysValues = getPartitionKeysValues(table, newPartition);
                for (String partitionKey: partitionKeysValues.keySet()) {
                    StringBuilderWriter addedPartitions = new StringBuilderWriter();
                    JsonWriter writer = new JsonWriter(addedPartitions);
                    writer.beginObject();
                    partitionKeysValues.get(partitionKey).forEach(pv -> {
                        try {
                            writer.name(partitionKey).value(pv);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    writer.endObject();
                    writer.close();
                    Metadata.EntityInfo entityInfo = new Metadata.EntityInfo(
                            String.format("%s.%s.%s", table.getDbName(), table.getTableName(), partitionKey),
                            table.getSd().getCols().stream().filter(c -> Objects.equals(c.getName(), partitionKey))
                                    .map(FieldSchema::getComment).reduce((x, y) -> String.format("%s.%s", x, y))
                                    .orElse("PartitionColumn"),
                            table.getSd().getLocation(),
                            table.getSd().getCols(),
                            table.getParameters(),
                            table.getOwner(),
                            table.getOwnerType(),
                            table.isSetCatName() ? table.getCatName() : DEFAULT_CATALOG_NAME,
                            jsonParser.parse(addedPartitions.toString())
                    );
                    Metadata metadata = new Metadata(Metadata.EntityType.PARTITION,
                            getEntityFQN(entityInfo),
                            EventMessage.EventType.ALTER_PARTITION.toString(),
                            System.currentTimeMillis(),
                            entityInfo);
                    send(metadata);
                }
            }catch (Exception e) {
                LOG.warn("Failed to get metadata, query was not affected. errMsg: {}", e.getMessage());
            }
        }
    }

    // 4. Function相关元数据变更

    /**
     * 新增函数 listener
     * eg:
     * @param fnEvent function event
     */
    @Override
    public void onCreateFunction(CreateFunctionEvent fnEvent) {
        if (fnEvent.getStatus()) {
            try {
                Function fn = fnEvent.getFunction();
                Description annotation = fn.getClass().getAnnotation(Description.class);
                Metadata.EntityInfo entityInfo = new Metadata.EntityInfo(
                        String.format("%s.%s", fn.getDbName(), fn.getFunctionName()),
                        annotation == null ? "" : String.format("%s: %s %s", annotation.name(), annotation.value(), annotation.extended()),
                        fn.getResourceUrisSize() == 0 ? "" : fn.getResourceUris().stream()
                                .map(ResourceUri::getUri)
                                .reduce((x, y) -> String.format("%s,%s", x, y))
                                .orElse(""),
                        null,
                        null,
                        fn.getOwnerName(),
                        fn.getOwnerType(),
                        fn.isSetCatName() ? fn.getCatName() : DEFAULT_CATALOG_NAME,
                        jsonParser.parse("{}")
                );
                Metadata metadata = new Metadata(Metadata.EntityType.FUNCTION,
                        getEntityFQN(entityInfo),
                        EventMessage.EventType.CREATE_FUNCTION.toString(),
                        System.currentTimeMillis(),
                        entityInfo);
                send(metadata);
            } catch (Exception e) {
                LOG.warn("Failed to get metadata, query was not affected. errMsg: {}", e.getMessage());
            }
        }
    }

    /**
     * 删除函数 listener
     * @param fnEvent function event
     */
    @Override
    public void onDropFunction(DropFunctionEvent fnEvent) {
        if (fnEvent.getStatus()) {
            try {
                Function fn = fnEvent.getFunction();
                Description annotation = fn.getClass().getAnnotation(Description.class);
                Metadata.EntityInfo entityInfo = new Metadata.EntityInfo(
                        String.format("%s.%s", fn.getDbName(), fn.getFunctionName()),
                        annotation == null ? "" : String.format("%s: %s %s", annotation.name(), annotation.value(), annotation.extended()),
                        fn.getResourceUrisSize() == 0 ? "" : fn.getResourceUris().stream()
                                .map(ResourceUri::getUri)
                                .reduce((x, y) -> String.format("%s,%s", x, y))
                                .orElse(""),
                        null,
                        null,
                        fn.getOwnerName(),
                        fn.getOwnerType(),
                        fn.isSetCatName() ? fn.getCatName() : DEFAULT_CATALOG_NAME,
                        jsonParser.parse("{}")
                );
                Metadata metadata = new Metadata(Metadata.EntityType.FUNCTION,
                        getEntityFQN(entityInfo),
                        EventMessage.EventType.DROP_FUNCTION.toString(),
                        System.currentTimeMillis(),
                        entityInfo);
                send(metadata);
            } catch (Exception e) {
                LOG.warn("Failed to get metadata, query was not affected. errMsg: {}", e.getMessage());
            }
        }
    }

    // 5. Catalog相关元数据变更 (TODO)

    /**
     * 创建catalog listener
     * @param catalogEvent CreateCatalogEvent
     */
    @Override
    public void onCreateCatalog(CreateCatalogEvent catalogEvent) {
        if (catalogEvent.getStatus()) {
            try {
                Catalog catalog = catalogEvent.getCatalog();
                Metadata.EntityInfo entityInfo = new Metadata.EntityInfo(
                        catalog.getName(),
                        catalog.getDescription(),
                        catalog.getLocationUri(),
                        null,
                        null,
                        null,
                        null,
                        catalog.getName(),
                        jsonParser.parse("{}")
                );
                Metadata metadata = new Metadata(Metadata.EntityType.CATALOG,
                        catalog.getName(),
                        EventMessage.EventType.CREATE_CATALOG.toString(),
                        System.currentTimeMillis(),
                        entityInfo);
                send(metadata);
            }catch (Exception e) {
                LOG.warn("Failed to get metadata, query was not affected. errMsg: {}", e.getMessage());
            }
        }
    }

    /**
     * 修改catalog listener
     * @param catalogEvent AlterCatalogEvent
     */
    @Override
    public void onAlterCatalog(AlterCatalogEvent catalogEvent) {
        if (catalogEvent.getStatus()) {
            try {
                Catalog catalog = catalogEvent.getNewCatalog();
                Metadata.EntityInfo entityInfo = new Metadata.EntityInfo(
                        catalog.getName(),
                        catalog.getDescription(),
                        catalog.getLocationUri(),
                        null,
                        null,
                        null,
                        null,
                        catalog.getName(),
                        jsonParser.parse("{}")
                );
                Metadata metadata = new Metadata(Metadata.EntityType.CATALOG,
                        catalog.getName(),
                        EventMessage.EventType.ALTER_CATALOG.toString(),
                        System.currentTimeMillis(),
                        entityInfo);
                send(metadata);
            }catch (Exception e) {
                LOG.warn("Failed to get metadata, query was not affected. errMsg: {}", e.getMessage());
            }
        }
    }

    /**
     * 删除catalog listener
     * @param catalogEvent DropCatalogEvent
     */
    @Override
    public void onDropCatalog(DropCatalogEvent catalogEvent) {
        if (catalogEvent.getStatus()) {
            try {
                Catalog catalog = catalogEvent.getCatalog();
                Metadata.EntityInfo entityInfo = new Metadata.EntityInfo(
                        catalog.getName(),
                        catalog.getDescription(),
                        catalog.getLocationUri(),
                        null,
                        null,
                        null,
                        null,
                        catalog.getName(),
                        jsonParser.parse("{}")
                );
                Metadata metadata = new Metadata(Metadata.EntityType.CATALOG,
                        catalog.getName(),
                        EventMessage.EventType.DROP_CATALOG.toString(),
                        System.currentTimeMillis(),
                        entityInfo);
                send(metadata);
            }catch (Exception e) {
                LOG.warn("Failed to get metadata, query was not affected. errMsg: {}", e.getMessage());
            }
        }
    }

    @Override
    public void onInsert(InsertEvent insertEvent) {

    }

    /**
     * 获取分区字段以及对应分区值
     * @param table Table
     * @param iterator PartitionIterator
     * @return Map key为分区字段 value为分区列表
     */
    private Map<String, List<String>> getPartitionKeysValues(final Table table, Iterator<Partition> iterator) {
        Map<String, List<String>> result = new HashMap<>(table.getPartitionKeysSize());
        // 遍历分区字段key
        for (int i=0; i<table.getPartitionKeysSize(); ++i) {
            String partitionKey = table.getPartitionKeys().get(i).getName();
            // 遍历分区字段value
            while (iterator.hasNext()) {
                Partition partition = iterator.next();
                String partitionValue = partition.getValues().get(i);
                if (result.get(partitionKey) == null) {
                    List<String> values = new ArrayList<>();
                    values.add(partitionValue);
                    result.put(partitionKey, values);
                } else {
                    result.get(partitionKey).add(partitionValue);
                }
            }
        }
        return result;
    }

    /**
     * 获取分区字段以及对应分区值
     * @param table Table
     * @param partition Partition
     * @return Map key为分区字段 value为分区列表
     */
    private Map<String, List<String>> getPartitionKeysValues(final Table table, Partition partition) {
        Map<String, List<String>> result = new HashMap<>(table.getPartitionKeysSize());
        // 遍历分区字段key
        for (int i=0; i<table.getPartitionKeysSize(); ++i) {
            String partitionKey = table.getPartitionKeys().get(i).getName();
            String partitionValue = partition.getValues().get(i);
            if (result.get(partitionKey) == null) {
                List<String> values = new ArrayList<>();
                values.add(partitionValue);
                result.put(partitionKey, values);
            } else {
                result.get(partitionKey).add(partitionValue);
            }
        }
        return result;
    }

    /**
     * 实体的FQN
     * @param entityInfo EntityInfo object
     * @return fqn 唯一标识符 catalogName.entityName
     */
    private String getEntityFQN(Metadata.EntityInfo entityInfo) {
        List<String> names = new ArrayList<>();
        names.add(entityInfo.getCatalogName() == null || Objects.equals(entityInfo.getCatalogName(), "")
                ? "hive"
                : entityInfo.getCatalogName());
        names.add(entityInfo.getName());
        return StringUtils.join(names, ".");
    }


    /**
     * 发送元数据变更信息到rest接口
     * @param metadata 元数据信息bean
     */
    private void send(Metadata metadata) {
        try {
            String postData = gson.toJson(metadata);
            LOG.info("==================" + postData);
            String res = HttpClient.doPost(META_SERVER_REST, postData, new HashMap<>(0));
            LOG.info("MetadataListener post res:{}", res);
        }catch (Exception e) {
            LOG.error("MetadataListener send data failed.errMsg: {}", e.getMessage());
        }
    }


}
