package com.kaka.group.cassandra.test;


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.kaka.group.config.CassandraConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class BasicOperationTest {

    private Session session;
    CassandraConfig config = null;

    @Before
    public void init(){
         config = new CassandraConfig();
        config.connect("localhost", 9042);
        this.session = config.getSession();
    }

    @Test
    public void keySpaceTest(){
        ResultSet result =
                session.execute("SELECT * FROM system_schema.keyspaces;");

        List<String> matchedKeyspaces = result.all()
                .stream()
                .filter(r -> r.getString(0).equals("firstkeyspace".toLowerCase()))
                .map(r -> r.getString(0))
                .collect(Collectors.toList());

        assertEquals(matchedKeyspaces.size(), 1);

        System.out.println("The Size ==== >>" +matchedKeyspaces.size());
        matchedKeyspaces.stream().forEach( e -> {
            System.out.println(e);
        });
        assertTrue(matchedKeyspaces.get(0).equals("firstkeyspace".toLowerCase()));
    }

    @Test
    public void alterTableName(){



      ResultSet rs =  config.alterTableName
              ("firstkeyspace.emp" , "emp_add" , "varchar" );

        boolean columnExists = rs.getColumnDefinitions().asList().stream()
                .anyMatch(cl -> cl.getName().equals("emp_add"));

      //assertTrue(columnExists);

    }


    }


