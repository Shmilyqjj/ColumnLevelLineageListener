package top.qjj.shmily.lineage;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import java.util.Set;

/**
 * :Description: Hive表更新lastAccessTime hook
 * :@Author: 佳境Shmily
 * :Create Time: 2023/8/2 19:32
 * :Site: shmily-qjj.top
 * :References: org.apache.hadoop.hive.ql.hooks.UpdateInputAccessTimeHook  [只增加了try catch 解决异常]
 *         2023-08-02T19:29:27,608  INFO [fee06e51-d86f-4686-acf1-d4be127aebab main] reexec.ReOptimizePlugin: ReOptimization: retryPossible: false
 *         2023-08-02T19:29:27,609 ERROR [fee06e51-d86f-4686-acf1-d4be127aebab main] ql.Driver: FAILED: Hive Internal Error: org.apache.hadoop.hive.ql.metadata.InvalidTableException(Table not found _dummy_table)
 *         org.apache.hadoop.hive.ql.metadata.InvalidTableException: Table not found _dummy_table
 *         at org.apache.hadoop.hive.ql.metadata.Hive.getTable(Hive.java:1135)
 *         at org.apache.hadoop.hive.ql.metadata.Hive.getTable(Hive.java:1105)
 *         at org.apache.hadoop.hive.ql.hooks.UpdateInputAccessTimeHook$PreExec.run(UpdateInputAccessTimeHook.java:64)
 *         at org.apache.hadoop.hive.ql.HookRunner.invokeGeneralHook(HookRunner.java:296)
 *         at org.apache.hadoop.hive.ql.HookRunner.runPreHooks(HookRunner.java:273)
 *         at org.apache.hadoop.hive.ql.Driver.execute(Driver.java:2364)
 *         at org.apache.hadoop.hive.ql.Driver.runInternal(Driver.java:2099)
 *         at org.apache.hadoop.hive.ql.Driver.run(Driver.java:1797)
 *         at org.apache.hadoop.hive.ql.Driver.run(Driver.java:1791)
 *         at org.apache.hadoop.hive.ql.reexec.ReExecDriver.run(ReExecDriver.java:157)
 *         at org.apache.hadoop.hive.ql.reexec.ReExecDriver.run(ReExecDriver.java:218)
 *         at org.apache.hadoop.hive.cli.CliDriver.processLocalCmd(CliDriver.java:239)
 *         at org.apache.hadoop.hive.cli.CliDriver.processCmd(CliDriver.java:188)
 *         at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:402)
 *         at org.apache.hadoop.hive.cli.CliDriver.processLine(CliDriver.java:335)
 *         at org.apache.hadoop.hive.cli.CliDriver.executeDriver(CliDriver.java:787)
 *         at org.apache.hadoop.hive.cli.CliDriver.run(CliDriver.java:759)
 *         at org.apache.hadoop.hive.cli.CliDriver.main(CliDriver.java:683)
 *         at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
 *         at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
 *         at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
 *         at java.lang.reflect.Method.invoke(Method.java:498)
 *         at org.apache.hadoop.util.RunJar.run(RunJar.java:323)
 *         at org.apache.hadoop.util.RunJar.main(RunJar.java:236)
 * 2023-08-02T19:29:27,609  INFO [fee06e51-d86f-4686-acf1-d4be127aebab main] ql.Driver: Completed executing command(queryId=user_20230802192923_daf65682-5164-45ce-80f4-51e79c094973); Time taken: 1.045 seconds
 * :Usage:
 *    [hive-site.xml]hive.exec.pre.hooks=top.qjj.shmily.lineage.UpdateInputAccessTimeHook
 */

@Slf4j
public class UpdateInputAccessTimeHook implements ExecuteWithHookContext {

    private static final String LAST_ACCESS_TIME = "lastAccessTime";

    @Override
    public void run(HookContext hookContext) throws Exception {
        try {
            HiveConf conf = hookContext.getConf();
            Set<ReadEntity> inputs = hookContext.getQueryPlan().getInputs();

            Hive db;
            try {
                db = Hive.get(conf);
            } catch (HiveException e) {
                // ignore
                db = null;
                return;
            }

            int lastAccessTime = (int) (System.currentTimeMillis()/1000);

            for(ReadEntity re: inputs) {
                // Set the last query time
                ReadEntity.Type typ = re.getType();
                switch(typ) {
                    // It is possible that read and write entities contain a old version
                    // of the object, before it was modified by StatsTask.
                    // Get the latest versions of the object
                    case TABLE: {
                        String dbName = re.getTable().getDbName();
                        String tblName = re.getTable().getTableName();
                        Table t = db.getTable(dbName, tblName);
                        t.setLastAccessTime(lastAccessTime);
                        db.alterTable(dbName + "." + tblName, t, null);
                        break;
                    }
                    case PARTITION: {
                        String dbName = re.getTable().getDbName();
                        String tblName = re.getTable().getTableName();
                        Partition p = re.getPartition();
                        Table t = db.getTable(dbName, tblName);
                        p = db.getPartition(t, p.getSpec(), false);
                        p.setLastAccessTime(lastAccessTime);
                        db.alterPartition(dbName, tblName, p, null);
                        t.setLastAccessTime(lastAccessTime);
                        db.alterTable(dbName + "." + tblName, t, null);
                        break;
                    }
                    default:
                        // ignore dummy inputs
                        break;
                }
            }
        }catch (Exception e) {
            log.warn("Failed to set lastAccessTime, query is not affected.Error: {}", e.getMessage());
        }
    }
}
