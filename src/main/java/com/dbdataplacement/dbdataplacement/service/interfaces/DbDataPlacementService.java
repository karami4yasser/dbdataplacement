package com.dbdataplacement.dbdataplacement.service.interfaces;


import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public interface DbDataPlacementService {

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
    public boolean moveDataToArchiveTable(String sourceTable,String destinationTable) throws SQLException, IOException, ExecutionException, InterruptedException;
}
