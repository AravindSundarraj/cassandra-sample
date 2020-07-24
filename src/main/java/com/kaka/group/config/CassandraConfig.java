package com.kaka.group.config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.util.Objects;

public class CassandraConfig {

    private Cluster cluster;
    private Session session;

    public Session connect(String node , int port){
       Cluster.Builder b = Cluster.builder().addContactPoint(node);

        Objects.requireNonNull(port);
        b.withPort(port);
        cluster = b.build();
        session = cluster.connect();
        return session;


    }

    public ResultSet alterTableName(String TABLE_NAME, String columnName ,String columnType){
        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(TABLE_NAME).append(" ADD ")
                .append(columnName).append(" ")
                .append(columnType).append(";");

        String query = sb.toString();
       ResultSet rs =  this.session.execute(query);
       return rs;
    }

    public Session getSession(){
        return this.session;
    }

    public void close(){
        session.close();
        cluster.close();
    }


}
