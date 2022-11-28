package com.caiye.binlogsql.filter;

import java.util.function.Predicate;

@SuppressWarnings("AlibabaClassMustHaveAuthor")
public interface SqlFilter extends Predicate<String> {

    @Override
    boolean test(String sql);
}