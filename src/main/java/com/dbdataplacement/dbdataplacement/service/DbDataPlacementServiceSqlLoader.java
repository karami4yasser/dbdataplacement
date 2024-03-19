package com.dbdataplacement.dbdataplacement.service;

import com.dbdataplacement.dbdataplacement.service.interfaces.DbDataPlacementService;
import com.dbdataplacement.dbdataplacement.utils.DBUtils;
import com.dbdataplacement.dbdataplacement.utils.OracleSqlLoader;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

@Service
public class DbDataPlacementServiceSqlLoader implements DbDataPlacementService {

    private final DBUtils dbUtils;

    public DbDataPlacementServiceSqlLoader(DBUtils dbUtils) {
        this.dbUtils = dbUtils;
    }

    /**
     *
     * @param sourceTable
     * @param destinationTable
     * @return
     * @throws SQLException
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Override
    public boolean moveDataToArchiveTable(String sourceTable, String destinationTable) throws SQLException, IOException, ExecutionException, InterruptedException {
        return dbUtils.sqlLoaderArchive(sourceTable,destinationTable);
    }
}
