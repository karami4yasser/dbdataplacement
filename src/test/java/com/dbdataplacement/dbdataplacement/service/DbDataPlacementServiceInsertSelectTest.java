package com.dbdataplacement.dbdataplacement.service;

import com.dbdataplacement.dbdataplacement.utils.DBUtils;
import com.dbdataplacement.dbdataplacement.utils.TableProperties;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DbDataPlacementServiceInsertSelectTest {
    @Autowired
    private  DBUtils  dbUtils;
    @Value("${tables.eft_tra.name}")
    private  String EFT_TRA_NAME;
    @Value("${tables.eft_tra.archiveTableName}")
    private  String EFT_TRA_ARCHIVE_NAME;
    @Value("${tables.eft_tra.condition}")
    private  String EFT_TRA_CONDITION;

    @Value("${tables.eft_aud.name}")
    private  String EFT_AUD_NAME;
    @Value("${tables.eft_aud.archiveTableName}")
    private  String EFT_AUD_ARCHIVE_NAME;
    @Value("${tables.eft_aud.condition}")
    private  String EFT_AUD_CONDITION;


    private Map<String, Object> originalTableStatistics_EFT_TRA = new HashMap<>();

    private Map<String, Object> archiveTableStatistics_EFT_TRA = new HashMap<>();

    private Map<String, Object> originalTableStatistics_EFT_AUD = new HashMap<>();

    private Map<String, Object> archiveTableStatistics_EFT_AUD = new HashMap<>();

    @Test
    @Order(0)
    public void init() throws FileNotFoundException {
        TableProperties.getTables();
        this.originalTableStatistics_EFT_TRA =dbUtils.generateTableStatistics(EFT_TRA_NAME,EFT_TRA_CONDITION);
        this.originalTableStatistics_EFT_AUD =dbUtils.generateTableStatistics(EFT_AUD_NAME,EFT_AUD_CONDITION);
    }

    @Test
    @Order(1)
    public void insertIntoSelect_EFT_TRA() {
        boolean result = dbUtils.insertIntoSelectAndDelete(EFT_TRA_NAME,EFT_TRA_ARCHIVE_NAME,EFT_TRA_CONDITION);
        assertEquals(Boolean.TRUE,result);
    }
    @Test
    @Order(2)
    public void compareStatistics_EFT_TRA() {
        for (String key : this.originalTableStatistics_EFT_TRA.keySet()) {
            assertEquals(this.archiveTableStatistics_EFT_TRA.get(key),this.originalTableStatistics_EFT_TRA.get(key));
        }
    }
    @Test
    @Order(3)
    public void insertIntoSelect_EFT_AUD() {
        boolean result = dbUtils.insertIntoSelectAndDelete(EFT_TRA_NAME,EFT_AUD_ARCHIVE_NAME,EFT_AUD_CONDITION);
        assertEquals(Boolean.TRUE,result);
    }
    @Test
    @Order(4)
    public void compareStatistics_EFT_AUD() {
        for (String key : this.originalTableStatistics_EFT_AUD.keySet()) {
            assertEquals(this.archiveTableStatistics_EFT_AUD.get(key),this.originalTableStatistics_EFT_AUD.get(key));
        }
    }



}
