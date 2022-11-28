package com.caiye.binlogsql.filter;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;

import java.util.function.Predicate;

@SuppressWarnings("AlibabaClassMustHaveAuthor")
public interface EventFilter extends Predicate<Event> {

    @Override
    default boolean test(Event event) {
        return true;
    }

    class TimestampEventFilter implements EventFilter {

        private long startTime;

        public TimestampEventFilter(long startTime) {
            this.startTime = startTime;
        }

        @Override
        public boolean test(Event event) {
            return startTime <= event.getHeader().getTimestamp();
        }
    }

    class PositionEventFilter implements EventFilter {

        private long position;

        public PositionEventFilter(long position) {
            this.position = position;
        }

        @Override
        public boolean test(Event event) {
            return position <= ((EventHeaderV4) event.getHeader()).getPosition();
        }
    }
}