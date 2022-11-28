package com.caiye.binlogsql.vo;

import lombok.Data;

@SuppressWarnings({"AlibabaClassMustHaveAuthor", "unused"})
@Data
public class DbInfoVo {

    private String host;
    private Integer port;
    private String username;
    private String password;

    public DbInfoVo() {

    }

    public DbInfoVo(String host, Integer port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

}
