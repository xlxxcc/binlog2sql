package com.caiye.binlogsql;

import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.RowsQueryEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.caiye.binlogsql.filter.CommonFilter;
import com.caiye.binlogsql.filter.EventFilter;
import com.caiye.binlogsql.filter.Filter;
import com.caiye.binlogsql.parser.LocalBinlog2Sql;
import com.caiye.binlogsql.tool.FileTool;
import com.caiye.binlogsql.vo.DbInfoVo;
import com.caiye.binlogsql.vo.FilterDbTableVo;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Reading binary log file to sql
 */
@SuppressWarnings({"serial", "AlibabaClassMustHaveAuthor"})
@Slf4j
public class LocalBinlog2SqlApplication {

    private static Filter filter = new CommonFilter();
    private static List<String> queryDmls = new ArrayList<>();

    static {
        List<FilterDbTableVo> includeDbTableVos = new ArrayList<FilterDbTableVo>() {{
            add(new FilterDbTableVo("apolloconfigdb_sit", "appnamespace"));
            add(new FilterDbTableVo("apolloconfigdb_sit", "namespace"));
            add(new FilterDbTableVo("apolloconfigdb_sit", "instanceconfig"));
            add(new FilterDbTableVo("apolloconfigdb_sit", "commit"));
            add(new FilterDbTableVo("apolloconfigdb_sit", "release"));
            add(new FilterDbTableVo("apolloconfigdb_sit", "releasemessage"));
            add(new FilterDbTableVo("apolloconfigdb_sit", "releasehistory"));
        }};
        ((CommonFilter) filter).setIncludeDbTableVos(includeDbTableVos);
        ((CommonFilter) filter).setEventFilters(Collections.singletonList(new EventFilter.PositionEventFilter(554948248L)));
        ((CommonFilter) filter).setSqlFilters(Arrays.asList(sql -> sql.contains("common.ns"), sql -> sql.contains("REPLACE(Message,'oauth-api','common')")));

        // 根据执行的dml sql语句找到需要回滚的sql
        queryDmls.add("update appnamespace set AppId = 'common' where  AppId = 'oauth-api' and name like 'common.ns%'");
        queryDmls.add("update namespace set AppId = 'common' where  Appid = 'oauth-api' and NamespaceName like 'common.ns%'");
        queryDmls.add("update instanceconfig set ConfigAppId = 'common' where ConfigAppId = 'oauth-api' and ConfigNamespaceName like '%common.ns%'");
        queryDmls.add("update `commit` set AppId = 'common' where Appid = 'oauth-api' and NamespaceName like 'common.ns%'");
        queryDmls.add("update `release` set Appid = 'common' where Appid = 'oauth-api' and NamespaceName like 'common.ns%'");
        queryDmls.add("update releasemessage set Message = REPLACE(Message,'oauth-api','common') where message like '%oauth-api%'");
        queryDmls.add("update releasehistory set  common = 'comon'  where Appid = 'oauth-api' and NamespaceName like 'common.ns%'");
    }

    @SuppressWarnings({"AlibabaRemoveCommentedCode"})
    public static void main(String[] args) {
        String file = "./binlog.000009";

        DbInfoVo dbInfoVo = new DbInfoVo("localhost", 3306, "root", "root");

        EventDeserializer eventDeserializer = new EventDeserializer();
        /*eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );*/

        new LocalBinlog2Sql(file, dbInfoVo).setEventDeserializer(eventDeserializer).setFilter(filter).readAndListen(event -> {
            /*EventData data = event.getData();
            if (data != null && data.getClass().isAssignableFrom(RowsQueryEventData.class)) {
                RowsQueryEventData dmlData = (RowsQueryEventData) data;
                System.out.println(dmlData.getQuery());
            }*/
            EventData data = event.getData();
            if (data != null && data.getClass().isAssignableFrom(RowsQueryEventData.class)) {
                RowsQueryEventData dmlData = (RowsQueryEventData) data;
                String sql = dmlData.getQuery();
                EventHeaderV4 eventHeader = event.getHeader();
                String comment = String.format("start %s end %s time %s"
                        , eventHeader.getPosition()
                        , eventHeader.getNextPosition()
                        , formatDateTime(new Date(eventHeader.getTimestamp()), ZoneOffset.systemDefault()));
                //noinspection AlibabaAvoidComplexCondition
                if (containsQueryDml(queryDmls, sql)) {
                    System.out.println("fuck:" + dmlData.getQuery() + "# " + comment);
                    try {
                        FileTool.appendFileContent("commit.sql", "---------------");
                        FileTool.appendFileContent("commit.sql", dmlData.getQuery() + "# " + comment);
                        FileTool.appendFileContent("commit.sql", "---------------");

                        FileTool.appendFileContent("rollback.sql", "---------------");
                        FileTool.appendFileContent("rollback.sql", dmlData.getQuery() + "# " + comment);
                        FileTool.appendFileContent("rollback.sql", "---------------");
                    } catch (IOException e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        }, "commit.sql", "rollback.sql");
    }

    private static String formatDateTime(Date date, ZoneId zoneOffset) {
        Instant instant = Instant.ofEpochMilli(date.getTime());
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.ofInstant(instant, zoneOffset));
    }

    private static boolean containsQueryDml(List<String> queryDmls, String sql) {
        return queryDmls.contains(sql);
    }
}
