package com.dbdataplacement.dbdataplacement.utils;

import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Transactional
public class DBUtils {
    private final  EntityManagerFactory entityManagerFactory;
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

    private final   Map<String, TableProperties.TableConfig> tablesConfigMap = TableProperties.getTables();

    public DBUtils(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = entityManagerFactory;
    }

    /**
     *
     * @return status of operation success/failure
     * move data
     */
    public boolean moveDataAllTables() {
        long start = System.currentTimeMillis();


        List<Boolean> results = tablesConfigMap.keySet().stream().map(
                (tableConfigMap) -> moveDataTable(tablesConfigMap.get(tableConfigMap).getName(),tablesConfigMap.get(tableConfigMap).getArchiveTableName(),tablesConfigMap.get(tableConfigMap).getCondition())
        ).toList()
        ;
        long time = System.currentTimeMillis() - start;
        System.out.println("insertIntoSelectAndDelete for all tables took : " + time+ " ms");
        return results.stream().allMatch(Boolean.TRUE::equals);
    }

    /**
     *
     * @param sourceTable
     * @param destinationTable
     * @param condition
     * @return status of operation success/failure
     */
    public boolean moveDataTable(String sourceTable,String destinationTable,String condition) {
        long start = System.currentTimeMillis();

        boolean result = true;

        if(insertIntoSelect(sourceTable,destinationTable,condition)) {
            result= deleteAllFromTable(sourceTable,condition);
        }
        long time = System.currentTimeMillis() - start;
        System.out.println("insertIntoSelectAndDelete for table "+ sourceTable+ " took : " + time+ " ms");
        return result;
    }


    /**
     *
     * @param sourceTable
     * @param destinationTable
     * @return status of operation success/failure
     * move data to destinationTable , then delete all moved data form sourceTable
     */
    public boolean insertIntoSelectAndDelete(String sourceTable,String destinationTable,String condition) {
    long start = System.currentTimeMillis();

    boolean result = true;

    if(insertIntoSelect(sourceTable,destinationTable,condition)) {
        result= deleteAllFromTable(sourceTable,condition);
    }
    long time = System.currentTimeMillis() - start;
    System.out.println("insertIntoSelectAndDelete for table "+ sourceTable+ " took : " + time+ " ms");
    return result;
}

    /**
     *
     * @param sourceTable
     * @param destinationTable
     * @return status of operation success/failure
     * insert data to the destinationTable using insert into select
     */
    public boolean insertIntoSelect(String sourceTable,String destinationTable,String condition) {

        EntityManager entityManager = entityManagerFactory.createEntityManager();
        Session session = entityManager.unwrap(Session.class);

        String INSERT_SELECT_QUERY = String.format("INSERT  INTO %s SELECT * FROM %s %s",destinationTable,sourceTable,condition);
        AtomicBoolean success = new AtomicBoolean(Boolean.TRUE);

        session.doWork(connection -> {
            try {
                long start = System.currentTimeMillis();
                connection.setAutoCommit(false);
                Statement statement = connection.createStatement();

                // Execute the insert query
                int rowsInserted = statement.executeUpdate(INSERT_SELECT_QUERY);


                // Commit the transaction
                connection.commit();
                // Close the resources
                statement.close();
                connection.close();
                long time = System.currentTimeMillis() - start;
                System.out.println(rowsInserted + " rows inserted into " + destinationTable+".");
                System.out.println("insertIntoSelect took : " + time+ " ms");


            }
            catch (Exception e) {
                connection.rollback();
                e.printStackTrace();
                success.set(Boolean.FALSE);
            }
            finally {
                try {
                    connection.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                    success.set(Boolean.FALSE);
                }

            }
        });

        return success.get();
    }

    /**
     *
     * @param sourceTable
     * @return status of operation success/failure
     * delete all table rows
     */
    public boolean deleteAllFromTable(String sourceTable,String condition) {

        EntityManager entityManager = entityManagerFactory.createEntityManager();
        Session session = entityManager.unwrap(Session.class);


        String DELETE_PARALLEL_QUERY = String.format("DELETE /*+ PARALLEL(%s,4) */ FROM %s %s",sourceTable,sourceTable,condition);
        String ALTER_SESSION_ENABLE_PARALLEL= "ALTER SESSION ENABLE PARALLEL DML";

        AtomicBoolean success = new AtomicBoolean(Boolean.TRUE);

        session.doWork(connection -> {
            try {
                long start = System.currentTimeMillis();
                connection.setAutoCommit(false);
                Statement statement = connection.createStatement();
                statement.execute(ALTER_SESSION_ENABLE_PARALLEL);


                // Execute the delete query
                int rowsDeleted = statement.executeUpdate(DELETE_PARALLEL_QUERY);

                // Commit the transaction
                connection.commit();
                // Close the resources
                statement.close();
                connection.close();
                long time = System.currentTimeMillis() - start;
                System.out.println("deleteAllFromTable took : " + time+ " ms");

                System.out.println(rowsDeleted + " rows deleted from "+ sourceTable +".");

            }
            catch (Exception e) {
                connection.rollback();
                e.printStackTrace();
                success.set(Boolean.FALSE);
            }
            finally {
                try {
                    connection.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                    success.set(Boolean.FALSE);
                }

            }
        });

        return success.get();
    }




    /**
     *
     * @param table
     * @return number rows
     * returns the number of rows in the specified table
     */
    public int getRowCount(String table,String condition) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        Session session = entityManager.unwrap(Session.class);
        AtomicInteger rowCount = new AtomicInteger(0);

        session.doWork(connection -> {
            try {
                Statement statement = connection.createStatement();
                String countQuery = String.format("SELECT COUNT(*) FROM %s %s", table,condition);
                ResultSet resultSet = statement.executeQuery(countQuery);

                if (resultSet.next()) {
                    rowCount.set(resultSet.getInt(1));
                }

                // Close the resources
                resultSet.close();
                statement.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        return rowCount.get();
    }

    /**
     *
     * @param table
     * @return map of tables statistics
     * generate some statistics about the table data , used for validation and testing purposes
     */
    public Map<String, Object> generateTableStatistics(String table,String condition) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        Session session = entityManager.unwrap(Session.class);
        Map<String, Object> statistics = new HashMap<>();

        session.doWork(connection -> {
            try {
                Statement statement = connection.createStatement();

                // Get the number of rows
                String countQuery = String.format("SELECT COUNT(*) FROM %s %s", table,condition);
                ResultSet countResult = statement.executeQuery(countQuery);
                if (countResult.next()) {
                    statistics.put("row_count", countResult.getInt(1));
                }


                // Close the resources
                countResult.close();
                statement.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        return statistics;
    }

}
