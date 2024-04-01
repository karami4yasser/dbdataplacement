package com.dbdataplacement.dbdataplacement.service;

import com.dbdataplacement.dbdataplacement.utils.DBUtils;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DbDataPlacementServiceInsertSelectTest {
    @Autowired
    private  DBUtils  dbUtils;

    private static final int NUMBER_OF_ROWS=100000;

    private Map<String, Object> originalTableStatistics = new HashMap<>();

    private Map<String, Object> archiveTableStatistics = new HashMap<>();

    @Test
    @Order(0)
    public void initData() {
        dbUtils.createTable("COUNTRIES");
        dbUtils.createTable("COUNTRIES_ARCHIVE");
        dbUtils.insertData("COUNTRIES",NUMBER_OF_ROWS);
        this.originalTableStatistics =dbUtils.generateTableStatistics("COUNTRIES");
        assertEquals(NUMBER_OF_ROWS,dbUtils.getRowCount("COUNTRIES"));
        assertEquals(0,dbUtils.getRowCount("COUNTRIES_ARCHIVE"));
    }

    @Test
    @Order(1)
    public void insertIntoSelect() {
        boolean result = dbUtils.insertIntoSelectAndDelete("COUNTRIES","COUNTRIES_ARCHIVE");
        assertEquals(Boolean.TRUE,result);
        this.archiveTableStatistics=dbUtils.generateTableStatistics("COUNTRIES_ARCHIVE");
        assertEquals(0,dbUtils.getRowCount("COUNTRIES"));
        assertEquals(NUMBER_OF_ROWS,dbUtils.getRowCount("COUNTRIES_ARCHIVE"));
    }

    @Test
    @Order(2)
    public void compareStatistics() {
        for (String key : this.originalTableStatistics.keySet()) {
            assertEquals(this.archiveTableStatistics.get(key),this.originalTableStatistics.get(key));
        }
    }

}
