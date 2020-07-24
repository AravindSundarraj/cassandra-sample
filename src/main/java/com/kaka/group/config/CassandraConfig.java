package com.kaka.group.config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.kaka.group.domain.Employee;

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

    public ResultSet insertData(Employee emp){

        StringBuilder
                sb = new StringBuilder("insert into ")
                .append(" firstkeyspace.emp").append("(emp_id,emp_add,emp_age," +
                        "emp_dept,emp_name,emp_ph_no)")
                .append("values(").append(emp.getEmpId())
                .append(",").append("'"+emp.getEmpAdd()+"'").append(",").
                        append(emp.getEmpAge()).append(",")
                .append("'"+emp.getEmpDept()+"'").append(",")
                .append("'"+emp.getEmpName()+"'").append(",")
                .append(emp.getEmpPhNo()).append(");");


        System.out.println("Print the query :" +sb.toString());

       ResultSet rs = session.execute(sb.toString());

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
