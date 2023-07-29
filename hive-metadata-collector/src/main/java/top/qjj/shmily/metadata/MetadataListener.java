package top.qjj.shmily.metadata;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.qjj.shmily.metadata.bean.Metadata;
import top.qjj.shmily.metadata.utils.HttpClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
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


    /**
     * 2023-07-30 00:00:21,520 INFO  org.apache.hadoop.hive.metastore.HiveMetaStore.audit: [pool-9-thread-6]: ugi=etl@SHMILY-QJJ.TOP   ip=10.2.5.101 cmd=create_table: Table(tableName:qjj_test_hahaha, dbName:default, owner:etl, createTime:1690646421, lastAccessTime:0, retention:0, sd:StorageDescriptor(cols:[FieldSchema(name:id, type:int, comment:null), FieldSchema(name:name, type:string, comment:null)], location:null, inputFormat:org.apache.hadoop.mapred.TextInputFormat, outputFormat:org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat, compressed:false, numBuckets:-1, serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, parameters:{serialization.format=1}), bucketCols:[], sortCols:[], parameters:{}, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}), storedAsSubDirectories:false), partitionKeys:[], parameters:{totalSize=0, numRows=0, rawDataSize=0, COLUMN_STATS_ACCURATE={"BASIC_STATS":"true"}, numFiles=0, numFilesErasureCoded=0}, viewOriginalText:null, viewExpandedText:null, tableType:MANAGED_TABLE, privileges:PrincipalPrivilegeSet(userPrivileges:{etl=[PrivilegeGrantInfo(privilege:INSERT, createTime:-1, grantor:etl, grantorType:USER, grantOption:true), PrivilegeGrantInfo(privilege:SELECT, createTime:-1, grantor:etl, grantorType:USER, grantOption:true), PrivilegeGrantInfo(privilege:UPDATE, createTime:-1, grantor:etl, grantorType:USER, grantOption:true), PrivilegeGrantInfo(privilege:DELETE, createTime:-1, grantor:etl, grantorType:USER, grantOption:true)]}, groupPrivileges:null, rolePrivileges:null), temporary:false, ownerType:USER)
     */

    private static final Logger LOG = LoggerFactory.getLogger(MetadataListener.class);
    private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private final static String META_SERVER_REST = "http://rest-host:8080/kafka/receive";

    public MetadataListener(final Configuration config) {
        super(config);
    }

    // 1. Database相关元数据变更

    @Override
    public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
        if (dbEvent.getStatus()) {
            try {
                Database database = dbEvent.getDatabase();
                String catalogName = database.getCatalogName();
                if (catalogName == null || "".equals(catalogName)) {
                    catalogName = "hive";
                }
                StringBuilderWriter ext = new StringBuilderWriter();
                JsonWriter writer = new JsonWriter(ext);
                try {
                    writer.beginObject();
                    if (database.getParametersSize() != 0) {
                        writer.name("parameters").value(database.getParameters().toString());
                    }
                    if (database.getPrivileges() != null) {
                        writer.name("privileges").value(database.getPrivileges().toString());
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
                        JsonParser.parseString(ext.toString())
                );

                Metadata metadata = new Metadata(Metadata.EntityType.DATABASE,
                        getEntityFQN(entityInfo),
                        EventMessage.EventType.CREATE_DATABASE.toString(),
                        entityInfo);
                send(metadata);
            } catch (Exception e) {
                LOG.warn("Failed to get metadata, query not affected. errMsg: {}", e.getMessage());
            }

        }
    }

    @Override
    public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
        if (dbEvent.getStatus()) {
            try {
                Database database = dbEvent.getDatabase();
                String catalogName = database.getCatalogName();
                if (catalogName == null || "".equals(catalogName)) {
                    catalogName = "hive";
                }
                StringBuilderWriter ext = new StringBuilderWriter();
                JsonWriter writer = new JsonWriter(ext);
                try {
                    writer.beginObject();
                    if (database.getParametersSize() != 0) {
                        writer.name("parameters").value(database.getParameters().toString());
                    }
                    if (database.getPrivileges() != null) {
                        writer.name("privileges").value(database.getPrivileges().toString());
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
                        JsonParser.parseString(ext.toString())
                );

                Metadata metadata = new Metadata(Metadata.EntityType.DATABASE,
                        getEntityFQN(entityInfo),
                        EventMessage.EventType.DROP_DATABASE.toString(),
                        entityInfo);
                send(metadata);
            }catch (Exception e) {
                LOG.warn("Failed to get metadata, query not affected. errMsg: {}", e.getMessage());
            }

        }
    }

    // 2. Table相关元数据变更

    @Override
    public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
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
                    if (t.getPrivileges() != null) {
                        writer.name("privileges").value(t.getPrivileges().toString());
                    }
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
                        t.getCatName(),
                        JsonParser.parseString(ext.toString())
                );

                Metadata metadata = new Metadata(Metadata.EntityType.TABLE,
                        getEntityFQN(entityInfo),
                        EventMessage.EventType.CREATE_TABLE.toString(),
                        entityInfo);
                send(metadata);
            } catch (Exception e) {
                LOG.warn("Failed to get metadata, query not affected. errMsg: {}", e.getMessage());
            }
        }

    }

//    /**
//     * @param tableEvent table event.
//     * @throws MetaException
//     */
//    @Override
//    public void onDropTable(DropTableEvent tableEvent) throws MetaException {
//        Table t = tableEvent.getTable();
//        NotificationEvent event =
//                new NotificationEvent(0, now(), EventMessage.EventType.DROP_TABLE.toString(), msgFactory
//                        .buildDropTableMessage(t).toString());
//        event.setCatName(t.isSetCatName() ? t.getCatName() : DEFAULT_CATALOG_NAME);
//        event.setDbName(t.getDbName());
//        event.setTableName(t.getTableName());
//        process(event, tableEvent);
//    }
//
//    /**
//     * @param tableEvent alter table event
//     * @throws MetaException
//     */
//    @Override
//    public void onAlterTable(AlterTableEvent tableEvent) throws MetaException {
//        Table before = tableEvent.getOldTable();
//        Table after = tableEvent.getNewTable();
//        NotificationEvent event =
//                new NotificationEvent(0, now(), EventMessage.EventType.ALTER_TABLE.toString(), msgFactory
//                        .buildAlterTableMessage(before, after, tableEvent.getIsTruncateOp()).toString());
//        event.setCatName(after.isSetCatName() ? after.getCatName() : DEFAULT_CATALOG_NAME);
//        event.setDbName(after.getDbName());
//        event.setTableName(after.getTableName());
//        process(event, tableEvent);
//    }
//
//    // 3. Partition相关元数据变更
//
//    /**
//     * @param partitionEvent partition event
//     * @throws MetaException
//     */
//    @Override
//    public void onAddPartition(AddPartitionEvent partitionEvent) throws MetaException {
//        Table t = partitionEvent.getTable();
//        String msg = msgFactory
//                .buildAddPartitionMessage(t, partitionEvent.getPartitionIterator(),
//                        new PartitionFilesIterator(partitionEvent.getPartitionIterator(), t)).toString();
//        NotificationEvent event =
//                new NotificationEvent(0, now(), EventMessage.EventType.ADD_PARTITION.toString(), msg);
//        event.setCatName(t.isSetCatName() ? t.getCatName() : DEFAULT_CATALOG_NAME);
//        event.setDbName(t.getDbName());
//        event.setTableName(t.getTableName());
//        process(event, partitionEvent);
//    }
//
//    /**
//     * @param partitionEvent partition event
//     * @throws MetaException
//     */
//    @Override
//    public void onDropPartition(DropPartitionEvent partitionEvent) throws MetaException {
//        Table t = partitionEvent.getTable();
//        NotificationEvent event =
//                new NotificationEvent(0, now(), EventMessage.EventType.DROP_PARTITION.toString(), msgFactory
//                        .buildDropPartitionMessage(t, partitionEvent.getPartitionIterator()).toString());
//        event.setCatName(t.isSetCatName() ? t.getCatName() : DEFAULT_CATALOG_NAME);
//        event.setDbName(t.getDbName());
//        event.setTableName(t.getTableName());
//        process(event, partitionEvent);
//    }
//
//    /**
//     * @param partitionEvent partition event
//     * @throws MetaException
//     */
//    @Override
//    public void onAlterPartition(AlterPartitionEvent partitionEvent) throws MetaException {
//        Partition before = partitionEvent.getOldPartition();
//        Partition after = partitionEvent.getNewPartition();
//        NotificationEvent event =
//                new NotificationEvent(0, now(), EventMessage.EventType.ALTER_PARTITION.toString(), msgFactory
//                        .buildAlterPartitionMessage(partitionEvent.getTable(), before, after, partitionEvent.getIsTruncateOp()).toString());
//        event.setCatName(before.isSetCatName() ? before.getCatName() : DEFAULT_CATALOG_NAME);
//        event.setDbName(before.getDbName());
//        event.setTableName(before.getTableName());
//        process(event, partitionEvent);
//    }
//
//    // 4. Function相关元数据变更
//
//    /**
//     * @param fnEvent function event
//     * @throws MetaException
//     */
//    @Override
//    public void onCreateFunction(CreateFunctionEvent fnEvent) throws MetaException {
//        Function fn = fnEvent.getFunction();
//        NotificationEvent event =
//                new NotificationEvent(0, now(), EventMessage.EventType.CREATE_FUNCTION.toString(), msgFactory
//                        .buildCreateFunctionMessage(fn).toString());
//        event.setCatName(fn.isSetCatName() ? fn.getCatName() : DEFAULT_CATALOG_NAME);
//        event.setDbName(fn.getDbName());
//        process(event, fnEvent);
//    }
//
//    /**
//     * @param fnEvent function event
//     * @throws MetaException
//     */
//    @Override
//    public void onDropFunction(DropFunctionEvent fnEvent) throws MetaException {
//        Function fn = fnEvent.getFunction();
//        NotificationEvent event =
//                new NotificationEvent(0, now(), EventMessage.EventType.DROP_FUNCTION.toString(), msgFactory
//                        .buildDropFunctionMessage(fn).toString());
//        event.setCatName(fn.isSetCatName() ? fn.getCatName() : DEFAULT_CATALOG_NAME);
//        event.setDbName(fn.getDbName());
//        process(event, fnEvent);
//    }
//
//    // 5. Catalog相关元数据变更
//
//    @Override
//    public void onCreateCatalog(CreateCatalogEvent catalogEvent) throws MetaException {
//
//    }
//
//    @Override
//    public void onAlterCatalog(AlterCatalogEvent catalogEvent) throws MetaException {
//
//    }
//
//    @Override
//    public void onDropCatalog(DropCatalogEvent catalogEvent) throws MetaException {
//
//    }
//
//    @Override
//    public void onInsert(InsertEvent insertEvent) {
//
//    }

    /**
     * 实体的FQN
     * @param entityInfo EntityInfo object
     * @return fqn 唯一标识符
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
