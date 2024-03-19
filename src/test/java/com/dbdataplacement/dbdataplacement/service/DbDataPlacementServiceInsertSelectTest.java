package com.dbdataplacement.dbdataplacement.service;

import com.dbdataplacement.dbdataplacement.service.DbDataPlacementServiceInsertSelect;
import com.dbdataplacement.dbdataplacement.utils.DBUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
public class DbDataPlacementServiceInsertSelectTest {
    @Autowired
    private DBUtils dbUtils;

    @BeforeEach
    public void init(){
        dbUtils.deleteAllFromTable("COUNTRIES");
        dbUtils.deleteAllFromTable("COUNTRIES_ARCHIVE");
        dbUtils.initData("COUNTRIES",1000);
    }
    @Test
    public void insertIntoSelect() {
        boolean result = dbUtils.insertIntoSelectAndDelete("COUNTRIES","COUNTRIES_ARCHIVE");
        assertEquals(Boolean.TRUE,result);
    }
}
