package com.dbdataplacement.dbdataplacement.service;

import com.dbdataplacement.dbdataplacement.service.interfaces.DbDataPlacementService;
import com.dbdataplacement.dbdataplacement.utils.DBUtils;
import org.springframework.stereotype.Service;

@Service
public class DbDataPlacementServiceInsertSelect implements DbDataPlacementService
{

    private final DBUtils dbUtils;

    public DbDataPlacementServiceInsertSelect(DBUtils dbUtils) {
        this.dbUtils = dbUtils;
    }

    /**
     *
     * @param sourceTable
     * @param destinationTable
     * @return
     */
    @Override
    public boolean moveDataToArchiveTable(String sourceTable, String destinationTable) {
        return dbUtils.insertIntoSelectAndDelete(sourceTable,destinationTable);
    }
}
