package com.caiye.binlogsql;

import com.caiye.binlogsql.filter.CommonFilter;
import com.caiye.binlogsql.filter.EventFilter;
import com.caiye.binlogsql.handler.BinlogEventHandler;
import com.caiye.binlogsql.handler.InsertHandler;
import com.caiye.binlogsql.handler.UpdateHandler;
import com.caiye.binlogsql.parser.ReplicationStream2Sql;
import com.caiye.binlogsql.vo.DbInfoVo;
import com.caiye.binlogsql.vo.FilterDbTableVo;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.caiye.binlogsql.handler.DeleteHandler;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import javax.xml.bind.DatatypeConverter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"AlibabaClassMustHaveAuthor", "WeakerAccess", "unused"})
@Slf4j
public abstract class SqlTest {

    public static final String DB_NAME = "test2";
    public List<Map<String, Object>> beforeData = new ArrayList<>();

    public List<SqlTestVo> sqlTestVos = new ArrayList<>();
    private ReplicationStream2Sql replicationStream2Sql;

    public static void cleanData(List<TableTestPairVo> tableTestPairVos) throws Exception {
        for (TableTestPairVo tableTestPairVo : tableTestPairVos) {
            MysqlUtil.insertOrUpdate("DELETE FROM " + tableTestPairVo.getOrgTableName());
            MysqlUtil.insertOrUpdate("DELETE FROM " + tableTestPairVo.getTestTableName());
        }
    }

    private static void injectProxy(ReplicationStream2Sql replicationStream2Sql, List<SqlTestVo> sqlTestVos) {
        Map<EventType, BinlogEventHandler> handleRegisterMap = replicationStream2Sql.getBinlogParser().getHandleRegisterMap();
        for (EventType eventType : handleRegisterMap.keySet()) {
            BinlogEventHandler orgBinlogEventHandle = handleRegisterMap.get(eventType);
            if (orgBinlogEventHandle instanceof InsertHandler
                    || orgBinlogEventHandle instanceof DeleteHandler
                    || orgBinlogEventHandle instanceof UpdateHandler)
                handleRegisterMap.put(eventType, new ProxyBinlogEventHandler(orgBinlogEventHandle, sqlTestVos));
        }
    }

    protected abstract List<TableTestPairVo> getTableTestPairVo();

    @BeforeEach
    void initAll() throws Exception {
        DbInfoVo dbInfoVo = new DbInfoVo("localhost", 3306, "root", "root");
        CommonFilter commonFilter = new CommonFilter()
                .setEventFilters(Collections.singletonList(new EventFilter.TimestampEventFilter(System.currentTimeMillis())))
                .setIncludeDbTableVos(
                        getTableTestPairVo().stream().map(tableTestPairVo -> new FilterDbTableVo(DB_NAME, tableTestPairVo.getOrgTableName())).collect(Collectors.toList()));

        replicationStream2Sql = new ReplicationStream2Sql(dbInfoVo)
                .setFilter(commonFilter)
                .connectAndListen(false, null, "commit.sql");
        injectProxy(replicationStream2Sql, sqlTestVos);

        cleanData(getTableTestPairVo());

        initData();
        Thread.sleep(3000);
        sqlTestVos.clear();
        log.info("数据清除完毕");

    }

    protected void initData() throws Exception {
    }

    @AfterEach
    private void close() {
        replicationStream2Sql.close();
        replicationStream2Sql = null;
    }

    public void saveBeforeDbDataTemporary(TableTestPairVo tableTestPairVo) throws Exception {
        List<Map<String, Object>> orgDatas = MysqlUtil.query("SELECT * FROM " + tableTestPairVo.getOrgTableName() + " ");
        changeByteArrayData(orgDatas);
        beforeData.clear();
        beforeData.addAll(orgDatas);
    }


    public void changeByteArrayData(List<Map<String, Object>> datas) {
        for (Map<String, Object> data : datas) {
            for (String key : data.keySet()) {
                if (data.get(key) instanceof byte[]) {
                    data.put(key, DatatypeConverter.printHexBinary((byte[]) data.get(key)));
                }
            }
        }
    }

    public void comparedBeforeTableData(TableTestPairVo tableTestPairVo) throws Exception {
        List<Map<String, Object>> testDatas = MysqlUtil.query("SELECT * FROM " + tableTestPairVo.getTestTableName() + " ");
        changeByteArrayData(testDatas);
        assertEquals(beforeData, testDatas);

    }

    public void comparedTableData(TableTestPairVo tableTestPairVo) throws Exception {
        List<Map<String, Object>> orgDatas = MysqlUtil.query("SELECT * FROM " + tableTestPairVo.getOrgTableName() + " ");
        List<Map<String, Object>> testDatas = MysqlUtil.query("SELECT * FROM " + tableTestPairVo.getTestTableName() + " ");
        changeByteArrayData(orgDatas);
        changeByteArrayData(testDatas);
        assertEquals(orgDatas, testDatas);

    }

    public void test(String fileName, TableTestPairVo tablePair) throws Exception {
        saveBeforeDbDataTemporary(tablePair);
        assertTrue(MysqlUtil.insertOrUpdateByFile(fileName) > 0);

        Thread.sleep(2000);
        for (SqlTestVo sqlTestVo : sqlTestVos) {
            for (String sql : sqlTestVo.getSqls()) {
                log.info("准备插入测试表 " + sql);
                assertTrue(MysqlUtil.insertOrUpdate(tablePair.replaceTableName(sql)) > 0);
            }
        }
        comparedTableData(tablePair);

        Thread.sleep(2000);
        log.info("准备逆向测试");
        for (SqlTestVo sqlTestVo : sqlTestVos) {
            for (String rollSql : sqlTestVo.getRollSqls()) {
                log.info("准备roll测试表 " + rollSql);
                assertTrue(MysqlUtil.insertOrUpdate(tablePair.replaceTableName(rollSql)) > 0);
            }
        }
        comparedBeforeTableData(tablePair);

    }
}
