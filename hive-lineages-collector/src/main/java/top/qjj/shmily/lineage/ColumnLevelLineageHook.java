package top.qjj.shmily.lineage;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.SetUtils;
import org.apache.commons.io.output.StringBuilderWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.*;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx.Index;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import top.qjj.shmily.lineage.bean.LineageMsg;
import top.qjj.shmily.lineage.utils.HttpClient;
import java.util.*;
import java.util.stream.Collectors;

/**
 * :Description: Hive列级血缘收集Hook (输出 表与表之间+列与列之间 的血缘关系包含计算和判断逻辑)
 * :@Author: 佳境Shmily
 * :Create Time: 2023/7/24 15:32
 * :Site: shmily-qjj.top
 * :References: org.apache.hadoop.hive.ql.hooks.LineageLogger
 * :Usage:
 *    [hive-site.xml]hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.LineageLogger,top.qjj.shmily.lineage.ColumnLevelLineageHook  (注意：org.apache.hadoop.hive.ql.hooks.LineageLogger必须设置，否则无法获取到列级别的血缘关系，原因详见org/apache/hadoop/hive/ql/optimizer/Optimizer.java:78)
 *    [hive-site.xml]column.lineage.enabled=true
 *    set job.fqn=your_job_name;  唯一job标识
 *    set mapred.job.name=your_job_name;   唯一job标识
 *    set column.lineage.job.description=your_description;  血缘的介绍信息（可选，非必须）
 *    set column.lineage.debug=true;   # 开启debug，不发kafka，只输出到hive client
 * :DEBUG command: hive --hiveconf hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.LineageLogger,top.qjj.shmily.lineage.ColumnLevelLineageHook --hiveconf column.lineage.enabled=true --hiveconf column.lineage.debug=true --hiveconf column.lineage.job.description=testjob
 * :血缘结果示例： {"fromTableFQN":"hive.default.dual_part","toTableFQN":"hive.default.dual_test","jobFQN":"HiveClient.qjj_test_job","lineage":[{"type":"COLUMN","from":["hive.default.dual_part.x"],"to":"hive.default.dual_test.a"},{"type":"COLUMN","from":["hive.default.dual_part.y"],"to":"hive.default.dual_test.b"},{"type":"COLUMN","from":["hive.default.dual_part.y","hive.default.dual_part.x"],"to":"hive.default.dual_test.c","expression":"sum((UDFToDouble(dual_part.y) + UDFToDouble(dual_part.x)))"},{"type":"TABLE","from":["hive.default.dual_part"],"to":"hive.default.dual_test","expression":"(UDFToDouble(dual_part.ds) = 2.0230718E7D)"}],"sqlQuery":"insert into default.dual_test select x,y,sum(y+x) from default.dual_part where ds = 20230718 group by x,y","queryTime":1690516964261,"queryDuration":63481,"execUser":"etl","execEngine":"hive-mr","extension":{"jobIds":["job_1676281359678_4230967"]}}
 */
@Slf4j
public class ColumnLevelLineageHook implements ExecuteWithHookContext {
    private static final HashSet<String> OPERATION_NAMES = new HashSet<>();
    private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private final static String KAFKA_TOPIC = "resolved_column_lineage";
    private final static String KAFKA_URL = "http://rest-host:8080/kafka/receive";

    // 捕获的操作类型
    static {
        OPERATION_NAMES.add(HiveOperation.QUERY.getOperationName());
        OPERATION_NAMES.add(HiveOperation.CREATETABLE_AS_SELECT.getOperationName());
        OPERATION_NAMES.add(HiveOperation.ALTERVIEW_AS.getOperationName());
        OPERATION_NAMES.add(HiveOperation.CREATEVIEW.getOperationName());
        OPERATION_NAMES.add(HiveOperation.LOAD.getOperationName());
    }

    @Data
    public static final class LineageData {
        private String fromTableFQN;
        private String toTableFQN;
        private String jobFQN;
        private List<Lineage> lineage;
        private String sqlQuery;
        private long queryTime;
        private long queryDuration;
        private String description;
        private String execUser;
        private String execEngine;
        private JsonElement extension;


        @AllArgsConstructor
        @Data
        public static class Lineage {
            public static enum Type {
                COLUMN,
                TABLE
            }
            private Lineage.Type type;
            private List<String> from;
            private String to;
            private String expression;
        }
    }

    /**
     * An edge in lineage.
     */
//    @VisibleForTesting
    public static final class Edge {
        public static enum Type {
            PROJECTION, PREDICATE
        }

        private Set<Vertex> sources;
        private Set<Vertex> targets;
        private String expr;
        private Type type;

        Edge(Set<Vertex> sources, Set<Vertex> targets, String expr, Type type) {
            this.sources = sources;
            this.targets = targets;
            this.expr = expr;
            this.type = type;
        }
    }

    /**
     * A vertex in lineage.
     */
//    @VisibleForTesting
    public static final class Vertex {

        /**
         * A type in lineage.
         */
        public static enum Type {
            COLUMN,
            TABLE
        }
        private Type type;
        private String label; // db.table.column
        private int id;

        Vertex(String label) {
            this(label, Type.COLUMN);
        }

        Vertex(String label, Type type) {
            this.label = label;
            this.type = type;
        }

        @Override
        public int hashCode() {
            return label.hashCode() + type.hashCode() * 3;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof Vertex)) {
                return false;
            }
            Vertex vertex = (Vertex) obj;
            return label.equals(vertex.label) && type == vertex.type;
        }

        public Type getType() {
            return type;
        }

        public String getLabel() {
            return label;
        }

        public int getId() {
            return id;
        }
    }

    @Override
    public void run(HookContext hookContext){
        assert (hookContext.getHookType() == HookContext.HookType.POST_EXEC_HOOK);
        HiveConf hiveConf = hookContext.getConf();
        boolean lineageEnabled = hiveConf.getBoolean("column.lineage.enabled", false);
        boolean lineageDebug = hiveConf.getBoolean("column.lineage.debug", false);
        log.info("column.lineage.enabled:{}  column.lineage.debug:{}", lineageEnabled, lineageDebug);
        if (!lineageEnabled) {
            return;
        }

        // Hive血缘解析
        QueryPlan plan = hookContext.getQueryPlan();
        Index index = hookContext.getIndex();
        SessionState ss = SessionState.get();
        if (ss != null && index != null && OPERATION_NAMES.contains(plan.getOperationName()) && !plan.isExplain()) {
            try {
                // 1. SQL string
                String queryStr = plan.getQueryStr().trim();

                // 校验是否为纯select语句
                List<org.apache.hadoop.hive.ql.metadata.Table> collect = plan.getOutputs().stream()
                        .map(Entity::getTable)
                        .filter(t -> !Objects.isNull(t))
                        .collect(Collectors.toList());
                if (collect.size() == 0) {
                    if(lineageDebug) {
                        log(String.format("[ColumnLevelLineageHook]select语句，无血缘关系 queryStr: \n%s", queryStr));
                    }
                    return;
                }

                // 2. execution user
                String execUser = hookContext.getUgi().getUserName();

                // 3. execution time and duration
                long queryTime = plan.getQueryStartTime();
                queryTime = queryTime == 0 ? System.currentTimeMillis() : queryTime;
                long duration = System.currentTimeMillis() - queryTime;

                // 4. job description set by user.
                String description = hiveConf.get("column.lineage.job.description");

                // 5. 任务唯一标识FQN
                String engineType = HiveConf.getVar(ss.getConf(), HiveConf.ConfVars.HIVE_EXECUTION_ENGINE);  // eg: mr/tez
                String jobFQN = getJobFQN(hiveConf);

                // 6. execEngine  执行引擎
                String execEngine = String.format("hive-%s", engineType);

                // 7. 扩展信息
                StringBuilderWriter extension = new StringBuilderWriter(1024);
                JsonWriter extensionWriter = new JsonWriter(extension);
                extensionWriter.beginObject();

                // 7.1 JobIds
                extensionWriter.name("jobIds");
                extensionWriter.beginArray();
                List<TaskRunner> tasks = hookContext.getCompleteTaskList();
                if (tasks != null && !tasks.isEmpty()) {
                    for (TaskRunner task: tasks) {
                        String jobId = task.getTask().getJobID();
                        if (jobId != null) {
                            extensionWriter.value(jobId);
                        }
                    }
                }
                extensionWriter.endArray();

                // 7.2 ErrorMessages
                extensionWriter.name("errMsg").value(hookContext.getErrorMessage());

                extensionWriter.endObject();
                extensionWriter.close();

                // 8. 血缘信息收集
                List<Edge> edges = getEdges(plan, index);

                if(lineageDebug) {
                    log("[ColumnLevelLineageHook]++queryStr:" + queryStr);
                    log("[ColumnLevelLineageHook]++execUser:" + execUser);
                    log("[ColumnLevelLineageHook]++execEngine:" + execEngine);
                    log("[ColumnLevelLineageHook]++queryTime:" + queryTime);
                    log("[ColumnLevelLineageHook]++duration:" + duration);
                    log("[ColumnLevelLineageHook]++description:" + description);
                    log("[ColumnLevelLineageHook]++jobFQN:" + jobFQN);
                    log("[ColumnLevelLineageHook]++extension:" + extension);
                    log("[ColumnLevelLineageHook]++Edges size:" + edges.size());
                }

                // 血缘信息解析处理
                Map<String, List<LineageData.Lineage>> lineagesGroupBySourceTable = new HashMap<>();
                String targetTable = null;
                List<String> tableLevelLineageExpressions = new ArrayList<>();
                List<String> sourceTables = new ArrayList<>();
                for (Edge edge:edges) {
                    switch (edge.type){
                        case PROJECTION:
                            // 映射类型Edge 生成字段关系 target只有一个字段（_c0这种的为临时输出字段名，因为无输出表，血缘不计） source有0到多个
                            List<Vertex> targets = new ArrayList<>(edge.targets);
                            List<String> fromColumns = edge.sources.stream().map(v -> String.format("hive.%s", v.label)).collect(Collectors.toList());
                            if (targets.size() == 0 || fromColumns.size() == 0) {
                                // 纯select语句 不生成血缘
                                if(lineageDebug) {
                                    log(String.format("[ColumnLevelLineageHook]select语句，无血缘关系 fromColumns.size:%s targets.size:%s",fromColumns.size(),targets.size()));
                                }
                                return;
                            }
                            String toColumn = targets.get(0).label;
                            if (Objects.equals(toColumn, "") || toColumn.matches("_c[0-9]+")) {
                                // 纯select语句 不生成血缘
                                if(lineageDebug) {
                                    log("[ColumnLevelLineageHook]select语句，无血缘关系 toColumn:" + toColumn);
                                }
                                return;
                            }
                            // 根据源表分组，分别存源表相关的字段血缘
                            List<String> srcDbTables = getDbTableFromVertexes(edge.sources);
                            List<String> targetTables = getDbTableFromVertexes(edge.targets);
                            if (targetTable == null && targetTables.size() != 0) {
                                targetTable = targetTables.get(0);
                            }
                            for (String srcDbTable: srcDbTables) {
                                LineageData.Lineage colLineage = new LineageData.Lineage(LineageData.Lineage.Type.COLUMN
                                        , fromColumns, String.format("hive.%s", toColumn), edge.expr);
                                if (!lineagesGroupBySourceTable.containsKey(srcDbTable)) {
                                    List<LineageData.Lineage> lineages = new ArrayList<>();
                                    lineages.add(colLineage);
                                    lineagesGroupBySourceTable.put(srcDbTable, lineages);
                                }else {
                                    lineagesGroupBySourceTable.get(srcDbTable).add(colLineage);
                                }
                            }
                            break;
                        case PREDICATE:
                            // 谓词条件类型Edge 包括join和where条件 生成表关系 target有多个（但只包含0或1张表） source有多个（可能相同或不同表）为where限制条件里的字段
                            if (edge.targets.size() == 0 || edge.targets.stream().anyMatch(v -> "_c0".equals(v.label))) {
                                // 纯select语句 不生成血缘
                                if(lineageDebug) {
                                    log("[ColumnLevelLineageHook]select语句 不生成血缘 edge.targets.size:" +  edge.targets.size());
                                }
                                return;
                            }
                            // 解析 sql conditions
                            if (edge.expr.matches("(.*_col[0-9]+ = .*_col[0-9]+.*)")) {
                                // 可能因为关联子查询操作导致无法解析到具体字段名，带上source中的字段以便分析血缘
                                tableLevelLineageExpressions.add(edge.sources
                                        .stream()
                                        .map(v -> String.format("hive.%s", v.label))
                                        .reduce((x,y) -> String.format("%s=%s", x,y))
                                        .orElse(edge.expr)
                                );
                            } else {
                                tableLevelLineageExpressions.add(edge.expr);
                            }
                            targetTables = getDbTableFromVertexes(edge.targets);
                            if (targetTable == null && targetTables.size() != 0) {
                                targetTable = targetTables.get(0);
                            }
                            if (Objects.equals(targetTable, "")) {
                                targetTable = targetTables.get(0);
                            }
                            for (String srcDbTable:getDbTableFromVertexes(edge.sources)) {
                                if (!sourceTables.contains(srcDbTable)) {
                                    sourceTables.add(srcDbTable);
                                }
                            }
                            break;
                        default:
                            throw new RuntimeException(String.format("Edge type %s is not supported.", edge.type));
                    }
                }

                if (targetTable == null || Objects.equals(targetTable, "")) {
                    if(lineageDebug){
                        log("[ColumnLevelLineageHook]**targetTable is null.");
                    }
                    return;
                }

                String tableExpr = StringUtils.join(tableLevelLineageExpressions, " # ");
                LineageData.Lineage tableLineage = new LineageData.Lineage(LineageData.Lineage.Type.TABLE, sourceTables, targetTable, tableExpr);

                if(lineageDebug){
                    log("[ColumnLevelLineageHook]++tableLineage:" + tableLineage);
                }

                // 生成最终输出结果
                List<LineageData> out = new ArrayList<>();
                for (String srcTable:lineagesGroupBySourceTable.keySet()) {
                    LineageData lineageData = new LineageData();
                    lineageData.setSqlQuery(queryStr);
                    lineageData.setExecEngine(execEngine);
                    lineageData.setExecUser(execUser);
                    lineageData.setQueryTime(queryTime);
                    lineageData.setQueryDuration(duration);
                    lineageData.setDescription(description);
                    lineageData.setJobFQN(jobFQN);
                    lineageData.setExtension(JsonParser.parseString(extension.toString()));

                    // lineages info (include columns and tables)
                    List<LineageData.Lineage> lineages = lineagesGroupBySourceTable.get(srcTable);
                    if (lineageDebug) {
                        log("[ColumnLevelLineageHook]++column lineages:");
                        lineages.stream().map(LineageData.Lineage::toString).forEach(ColumnLevelLineageHook::log);
                    }
                    // 表关系写入lineages
                    lineages.add(tableLineage);
                    lineageData.setLineage(lineages);
                    lineageData.setFromTableFQN(srcTable);
                    lineageData.setToTableFQN(targetTable);
                    out.add(lineageData);
                }

                for (LineageData ld : out) {
                    if (lineageDebug) {
                        // In debug mode, redirect to console output and do not send to kafka.
                        log("血缘结果数据：\n" + gson.toJson(ld));
                    }else {
                        // In non-debug mode, only send to kafka.
                        LineageMsg msg = new LineageMsg(KAFKA_TOPIC, gson.toJson(ld));
                        String postData = gson.toJson(msg);
                        String s = HttpClient.doPost(KAFKA_URL, postData, new HashMap<>(0));
                        log.info("column_level_lineage post res:{}", s);
                    }
                }
            } catch (Throwable t) {
                // Don't fail the query just because of any lineage issue.
                log("Failed to log lineage graph, query is not affected\n"
                        + org.apache.hadoop.util.StringUtils.stringifyException(t));
            }
        }else {
            log.info("No need to analyze lineages, ss!=null: {}, index!=null: {}  plan.getOperationName: {}, plan.isExplain: {}", ss!=null, index!=null, plan.getOperationName(), plan.isExplain());
        }
    }

    /**
     * Logger an error to console if available.
     */
    private static void log(String error) {
        LogHelper console = SessionState.getConsole();
        if (console != null) {
            console.printError(error);
        }
    }

    /**
     * Generate Job FQN
     * @param hiveConf HiveConf
     * @return jobFQN 任务唯一标识名
     */
    public static String getJobFQN(HiveConf hiveConf) {
        String jobFQN = hiveConf.get("job.fqn");
        String mapredJobName = hiveConf.get("mapred.job.name");
        String hiveQueryName = hiveConf.get("hive.query.name");
        if (jobFQN != null && !Objects.equals(jobFQN, "")) {
            return jobFQN;
        }else if (mapredJobName != null && !Objects.equals(mapredJobName, "")) {
            return mapredJobName;
        }else if (hiveQueryName != null && !Objects.equals(hiveQueryName, "")) {
            return hiveQueryName;
        }else {
            return "#UnknownHiveJobName#Please set job.fqn=your_job_name.";
        }
    }


    /**
     * Get db.Table from source or target vertexes
     * @param vertexes source or target vertexes
     * @return db.table list
     */
    public static List<String> getDbTableFromVertexes(Set<Vertex> vertexes) {
        List<String> res = new ArrayList<>();
        vertexes.forEach(v -> {
            String dbTable;
            String[] split = v.label.split("\\.");
            if (split.length == 3) {
                dbTable = String.format("hive.%s.%s", split[0], split[1]);
            }else {
                dbTable = String.format("hive.%s", v.label);
            }

            if (!res.contains(dbTable)) {
                res.add(dbTable);
            }
        });
        return res;
    }

    /**
     * Based on the final select operator, find out all the target columns.
     * For each target column, find out its sources based on the dependency index.
     */
    public static List<Edge> getEdges(QueryPlan plan, Index index) {
        LinkedHashMap<String, ObjectPair<SelectOperator, org.apache.hadoop.hive.ql.metadata.Table>> finalSelOps = index.getFinalSelectOps();
        Map<String, Vertex> vertexCache = new LinkedHashMap<String, Vertex>();
        List<Edge> edges = new ArrayList<Edge>();
        for (ObjectPair<SelectOperator,
                org.apache.hadoop.hive.ql.metadata.Table> pair: finalSelOps.values()) {
            List<FieldSchema> fieldSchemas = plan.getResultSchema().getFieldSchemas();
            SelectOperator finalSelOp = pair.getFirst();
            org.apache.hadoop.hive.ql.metadata.Table t = pair.getSecond();
            String destTableName = null;
            List<String> colNames = null;
            if (t != null) {
                destTableName = t.getFullyQualifiedName();
                fieldSchemas = t.getCols();
            } else {
                // Based on the plan outputs, find out the target table name and column names.
                for (WriteEntity output : plan.getOutputs()) {
                    Entity.Type entityType = output.getType();
                    if (entityType == Entity.Type.TABLE
                            || entityType == Entity.Type.PARTITION) {
                        t = output.getTable();
                        destTableName = t.getFullyQualifiedName();
                        List<FieldSchema> cols = t.getCols();
                        if (cols != null && !cols.isEmpty()) {
                            colNames = Utilities.getColumnNamesFromFieldSchema(cols);
                        }
                        break;
                    }
                }
            }
            Map<ColumnInfo, LineageInfo.Dependency> colMap = index.getDependencies(finalSelOp);
            List<LineageInfo.Dependency> dependencies = colMap != null ? Lists.newArrayList(colMap.values()) : null;
            int fields = fieldSchemas.size();
            if (t != null && colMap != null && fields < colMap.size()) {
                // Dynamic partition keys should be added to field schemas.
                List<FieldSchema> partitionKeys = t.getPartitionKeys();
                int dynamicKeyCount = colMap.size() - fields;
                int keyOffset = partitionKeys.size() - dynamicKeyCount;
                if (keyOffset >= 0) {
                    fields += dynamicKeyCount;
                    for (int i = 0; i < dynamicKeyCount; i++) {
                        FieldSchema field = partitionKeys.get(keyOffset + i);
                        fieldSchemas.add(field);
                        if (colNames != null) {
                            colNames.add(field.getName());
                        }
                    }
                }
            }
            if (dependencies == null || dependencies.size() != fields) {
                log("Result schema has " + fields
                        + " fields, but we don't get as many dependencies");
            } else {
                // Go through each target column, generate the lineage edges.
                Set<Vertex> targets = new LinkedHashSet<Vertex>();
                for (int i = 0; i < fields; i++) {
                    Vertex target = getOrCreateVertex(vertexCache,
                            getTargetFieldName(i, destTableName, colNames, fieldSchemas),
                            Vertex.Type.COLUMN);
                    targets.add(target);
                    LineageInfo.Dependency dep = dependencies.get(i);
                    addEdge(vertexCache, edges, dep.getBaseCols(), target,
                            dep.getExpr(), Edge.Type.PROJECTION);
                }
                Set<LineageInfo.Predicate> conds = index.getPredicates(finalSelOp);
                if (conds != null && !conds.isEmpty()) {
                    for (LineageInfo.Predicate cond: conds) {
                        addEdge(vertexCache, edges, cond.getBaseCols(),
                                new LinkedHashSet<Vertex>(targets), cond.getExpr(),
                                Edge.Type.PREDICATE);
                    }
                }
            }
        }
        return edges;
    }

    private static void addEdge(Map<String, Vertex> vertexCache, List<Edge> edges,
                                Set<LineageInfo.BaseColumnInfo> srcCols, Vertex target, String expr, Edge.Type type) {
        Set<Vertex> targets = new LinkedHashSet<Vertex>();
        targets.add(target);
        addEdge(vertexCache, edges, srcCols, targets, expr, type);
    }

    /**
     * Find an edge from all edges that has the same source vertices.
     * If found, add the more targets to this edge's target vertex list.
     * Otherwise, create a new edge and add to edge list.
     */
    private static void addEdge(Map<String, Vertex> vertexCache, List<Edge> edges,
                                Set<LineageInfo.BaseColumnInfo> srcCols, Set<Vertex> targets, String expr, Edge.Type type) {
        Set<Vertex> sources = createSourceVertices(vertexCache, srcCols);
        Edge edge = findSimilarEdgeBySources(edges, sources, expr, type);
        if (edge == null) {
            edges.add(new Edge(sources, targets, expr, type));
        } else {
            edge.targets.addAll(targets);
        }
    }

    /**
     * Convert a list of columns to a set of vertices.
     * Use cached vertices if possible.
     */
    private static Set<Vertex> createSourceVertices(
            Map<String, Vertex> vertexCache, Collection<LineageInfo.BaseColumnInfo> baseCols) {
        Set<Vertex> sources = new LinkedHashSet<Vertex>();
        if (baseCols != null && !baseCols.isEmpty()) {
            for(LineageInfo.BaseColumnInfo col: baseCols) {
                Table table = col.getTabAlias().getTable();
                if (table.isTemporary()) {
                    // Ignore temporary tables
                    continue;
                }
                Vertex.Type type = Vertex.Type.TABLE;
                String tableName = Warehouse.getQualifiedName(table);
                FieldSchema fieldSchema = col.getColumn();
                String label = tableName;
                if (fieldSchema != null) {
                    type = Vertex.Type.COLUMN;
                    label = tableName + "." + fieldSchema.getName();
                }
                sources.add(getOrCreateVertex(vertexCache, label, type));
            }
        }
        return sources;
    }

    /**
     * Find a vertex from a cache, or create one if not.
     */
    private static Vertex getOrCreateVertex(
            Map<String, Vertex> vertices, String label, Vertex.Type type) {
        Vertex vertex = vertices.get(label);
        if (vertex == null) {
            vertex = new Vertex(label, type);
            vertices.put(label, vertex);
        }
        return vertex;
    }

    /**
     * Find an edge that has the same type, expression, and sources.
     */
    private static Edge findSimilarEdgeBySources(
            List<Edge> edges, Set<Vertex> sources, String expr, Edge.Type type) {
        for (Edge edge: edges) {
            if (edge.type == type && StringUtils.equals(edge.expr, expr)
                    && SetUtils.isEqualSet(edge.sources, sources)) {
                return edge;
            }
        }
        return null;
    }

    /**
     * Generate normalized name for a given target column.
     */
    private static String getTargetFieldName(int fieldIndex,
                                             String destTableName, List<String> colNames, List<FieldSchema> fieldSchemas) {
        String fieldName = fieldSchemas.get(fieldIndex).getName();
        String[] parts = fieldName.split("\\.");
        if (destTableName != null) {
            String colName = parts[parts.length - 1];
            if (colNames != null && !colNames.contains(colName)) {
                colName = colNames.get(fieldIndex);
            }
            return destTableName + "." + colName;
        }
        if (parts.length == 2 && parts[0].startsWith("_u")) {
            return parts[1];
        }
        return fieldName;
    }

}

