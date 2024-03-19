package com.dbdataplacement.dbdataplacement.service;

import com.dbdataplacement.dbdataplacement.utils.DBUtils;
import com.dbdataplacement.dbdataplacement.utils.OracleSqlLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)

public class DbDataPlacementServiceSqlLoaderTest {

    @Autowired
    private DBUtils dbUtils;

    @BeforeEach
    public void init(){
        dbUtils.deleteAllFromTable("COUNTRIES");
        dbUtils.deleteAllFromTable("COUNTRIES_ARCHIVE");
        dbUtils.initData("COUNTRIES",1000);
    }
    @Test
    public void sqlLoader() throws SQLException, IOException, ExecutionException, InterruptedException {
        boolean result = dbUtils.sqlLoaderArchive("COUNTRIES","COUNTRIES_ARCHIVE");
        assertEquals(Boolean.TRUE,result);

    }
}
