package com.caiye.binlogsql.parser;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.caiye.binlogsql.handler.DeleteHandler;
import com.caiye.binlogsql.handler.InsertHandler;
import com.caiye.binlogsql.handler.TableMapHandler;
import com.caiye.binlogsql.handler.UpdateHandler;
import com.caiye.binlogsql.vo.DbInfoVo;
import com.caiye.binlogsql.filter.Filter;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings({"AlibabaClassMustHaveAuthor", "Duplicates"})
@Slf4j
@Accessors(chain = true)
public class ReplicationStream2Sql {

    private static ExecutorService THREAD_POOL = new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new BinlogParserThreadFactory("binglog-client-%d"),
            new ThreadPoolExecutor.AbortPolicy());

    @Getter
    private BinlogParser binlogParser = new BinlogParser();
    private DbInfoVo dbInfoVo;

    @Setter
    private Filter filter = new Filter() {
    };
    private BinaryLogClient binaryLogClient;

    static {
        //noinspection AlibabaRemoveCommentedCode
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
//            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error("没找到jdbc类, 无法使用MYSQL类");
        }
    }

    public ReplicationStream2Sql(DbInfoVo dbInfoVo) {
        this.dbInfoVo = dbInfoVo;
    }

    @SuppressWarnings({"LoopStatementThatDoesntLoop", "SqlNoDataSourceInspection"})
    private String getFirstBinLogName() {
        String url = "jdbc:mysql://" + dbInfoVo.getHost() + ":" + dbInfoVo.getPort() + "/mysql";
        try (Connection conn = DriverManager.getConnection(url, dbInfoVo.getUsername(), dbInfoVo.getPassword()); Statement statement = conn.createStatement()) {
            ResultSet resultSet = statement.executeQuery("show master logs;");
            while (resultSet.next()) {
                return resultSet.getString("Log_name");
            }
        } catch (SQLException e) {
            log.error("获取binlogName失败", e);
        }
        return null;
    }

    private void initBinlogEventHandler() {
        binlogParser.registerHandle(new InsertHandler(filter), EventType.WRITE_ROWS, EventType.EXT_WRITE_ROWS, EventType.PRE_GA_WRITE_ROWS);
        binlogParser.registerHandle(new DeleteHandler(filter), EventType.DELETE_ROWS, EventType.EXT_DELETE_ROWS, EventType.PRE_GA_DELETE_ROWS);
        binlogParser.registerHandle(new UpdateHandler(filter), EventType.UPDATE_ROWS, EventType.EXT_UPDATE_ROWS, EventType.PRE_GA_UPDATE_ROWS);
        binlogParser.registerHandle(new TableMapHandler(filter, dbInfoVo), EventType.TABLE_MAP);

        binlogParser.setFilter(filter);
    }

    public ReplicationStream2Sql connectAndListen(boolean isTurn, String contains, String fileName) {
        initBinlogEventHandler();

        binaryLogClient = new BinaryLogClient(dbInfoVo.getHost(),
                dbInfoVo.getPort(),
                dbInfoVo.getUsername(),
                dbInfoVo.getPassword());
        binaryLogClient.setServerId(1);
        binaryLogClient.setBinlogFilename(getFirstBinLogName());

        binaryLogClient.registerEventListener(event -> {
            if (!filter.filter(event)) {
                return;
            }
            binlogParser.handle(event, isTurn, fileName);
        });

        THREAD_POOL.execute(() -> {
            try {
                binaryLogClient.connect();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        });

        return this;
    }

    public void close() {
        try {
            if (binaryLogClient != null) {
                binaryLogClient.disconnect();
                binaryLogClient = null;
            }
        } catch (IOException e) {
            log.error("关闭失败", e);
        }
    }

    static class BinlogParserThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private String nameFormatter;

        BinlogParserThreadFactory(String nameFormatter) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.nameFormatter = nameFormatter;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, String.format(Locale.ROOT, nameFormatter, threadNumber.getAndIncrement()), 0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
