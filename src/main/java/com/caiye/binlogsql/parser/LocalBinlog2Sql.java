package com.caiye.binlogsql.parser;

import com.caiye.binlogsql.handler.InsertHandler;
import com.caiye.binlogsql.handler.TableMapHandler;
import com.caiye.binlogsql.handler.UpdateHandler;
import com.github.shyiko.mysql.binlog.BinaryLogFileReader;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.caiye.binlogsql.filter.Filter;
import com.caiye.binlogsql.handler.DeleteHandler;
import com.caiye.binlogsql.vo.DbInfoVo;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

@SuppressWarnings({"AlibabaClassMustHaveAuthor", "Duplicates", "unused", "UnusedReturnValue"})
@Slf4j
@Accessors(chain = true)
public class LocalBinlog2Sql {

    private String binlogFile;
    private DbInfoVo dbInfoVo;
    @Getter
    private BinlogParser binlogParser = new BinlogParser();
    @Setter
    private Filter filter = new Filter() {
    };
    @Setter
    private EventDeserializer eventDeserializer;

    static {
        //noinspection AlibabaRemoveCommentedCode
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
//            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            log.error("没找到jdbc类, 无法使用MYSQL类");
        }
    }

    public LocalBinlog2Sql(String binlogFile, DbInfoVo dbInfoVo) {
        this.binlogFile = binlogFile;
        this.dbInfoVo = dbInfoVo;
    }

    private void initBinlogEventHandler() {
        binlogParser.registerHandle(new InsertHandler(filter), EventType.WRITE_ROWS, EventType.EXT_WRITE_ROWS, EventType.PRE_GA_WRITE_ROWS);
        binlogParser.registerHandle(new DeleteHandler(filter), EventType.DELETE_ROWS, EventType.EXT_DELETE_ROWS, EventType.PRE_GA_DELETE_ROWS);
        binlogParser.registerHandle(new UpdateHandler(filter), EventType.UPDATE_ROWS, EventType.EXT_UPDATE_ROWS, EventType.PRE_GA_UPDATE_ROWS);
        binlogParser.registerHandle(new TableMapHandler(filter, dbInfoVo), EventType.TABLE_MAP);

        binlogParser.setFilter(filter);
    }

    public LocalBinlog2Sql readAndListen(Consumer<Event> consumer, String commitFileName, String rollbackSql) {
        initBinlogEventHandler();

        BinaryLogFileReader reader = null;
        try {
            File file = new File(binlogFile);
            reader = new BinaryLogFileReader(file, eventDeserializer);
            for (Event event; (event = reader.readEvent()) != null; ) {
                //log.info("consume position: " + ((EventHeaderV4)event.getHeader()).getPosition());
                consumer.accept(event);
                if (!filter.filter(event)) {
                    continue;
                }
                binlogParser.handle(event, false, commitFileName);
                binlogParser.handle(event, true, rollbackSql);
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        return this;
    }

    public LocalBinlog2Sql readAndListen(boolean isTurn, String fileName) {
        initBinlogEventHandler();

        BinaryLogFileReader reader = null;
        try {
            File file = new File(binlogFile);
            reader = new BinaryLogFileReader(file, eventDeserializer);
            for (Event event; (event = reader.readEvent()) != null; ) {
                if (!filter.filter(event)) {
                    continue;
                }
                binlogParser.handle(event, isTurn, fileName);
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        return this;
    }
}
