package com.caiye.binlogsql;

import com.caiye.binlogsql.parser.ReplicationStream2Sql;
import com.caiye.binlogsql.filter.CommonFilter;
import com.caiye.binlogsql.filter.EventFilter;
import com.caiye.binlogsql.vo.DbInfoVo;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

/**
 * Tapping into MySQL replication stream to sql
 */
@SuppressWarnings("AlibabaClassMustHaveAuthor")
@Slf4j
public class ReplicationStream2SqlApplication {

    public static void main(String[] args) {
        log.info("#############");

        DbInfoVo dbInfoVo = new DbInfoVo("localhost", 3306, "root", "root");
        new ReplicationStream2Sql(dbInfoVo)
                .setFilter(new CommonFilter().setEventFilters(Collections.singletonList(new EventFilter.TimestampEventFilter(System.currentTimeMillis()))))
                .connectAndListen(false, null, "commit.sql");

//        new ReplicationStream2Sql(dbInfoVo)
//                .setFilter(new CommonFilter().setEventFilters(Collections.singletonList(new EventFilter.TimestampEventFilter(System.currentTimeMillis()))))
//                .connectAndListen(true, null, "rollback.sql");
    }
}
