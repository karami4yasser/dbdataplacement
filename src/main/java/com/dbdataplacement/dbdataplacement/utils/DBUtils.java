package com.dbdataplacement.dbdataplacement.utils;

import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
@Transactional
public class DBUtils {
    private final  EntityManagerFactory entityManagerFactory;
    @Value("${database.connectionstring}")
    private String connectionstring;

    public DBUtils(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = entityManagerFactory;
    }

    /**
     *
     * @param sourceTable
     * @param destinationTable
     * @return
     */
    public boolean insertIntoSelectAndDelete(String sourceTable,String destinationTable) {
    long start = System.currentTimeMillis();

    boolean result = true;

    if(insertIntoSelect(sourceTable,destinationTable)) {
        result= deleteAllFromTable(sourceTable);
    }
    long time = System.currentTimeMillis() - start;
    System.out.println("insertIntoSelectAndDelete took : " + time+ " ms");
    return result;
}

    public boolean insertIntoSelect(String sourceTable,String destinationTable) {

        EntityManager entityManager = entityManagerFactory.createEntityManager();
        Session session = entityManager.unwrap(Session.class);

        String INSERT_PARALLEL_SELECT_QUERY = String.format("INSERT /*+ PARALLEL(%s,4) */ INTO %s SELECT * FROM %s",destinationTable,destinationTable,sourceTable);
        String DELETE_PARALLEL_QUERY = String.format("DELETE /*+ PARALLEL(%s,4) */ FROM %s",sourceTable,sourceTable);
        String INSERT_SELECT_QUERY = String.format("INSERT  INTO %s SELECT * FROM %s",destinationTable,sourceTable);
        String DELETE_QUERY = String.format("DELETE  FROM %s",sourceTable);
        String ALTER_SESSION_ENABLE_PARALLEL= "ALTER SESSION ENABLE PARALLEL DML";

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
     * @return
     */
    public boolean deleteAllFromTable(String sourceTable) {

        EntityManager entityManager = entityManagerFactory.createEntityManager();
        Session session = entityManager.unwrap(Session.class);

        String DELETE_QUERY = String.format("DELETE  FROM %s",sourceTable);
        String DELETE_PARALLEL_QUERY = String.format("DELETE /*+ PARALLEL(%s,4) */ FROM %s",sourceTable,sourceTable);
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
     * @param sourceTable
     * @param destinationTable
     * @return
     * @throws SQLException
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public Boolean sqlLoaderArchive(String sourceTable,String destinationTable) throws SQLException, IOException, ExecutionException, InterruptedException {
        String targetClassesDirPath = new File(DBUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getPath();
        long start = System.currentTimeMillis();
        int numberOfFile = OracleSqlLoader.exportDataToSqlLoaderFiles(entityManagerFactory,sourceTable,targetClassesDirPath);

        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*2);
        List<Future<Boolean>> futures = new ArrayList<>();

        for (int i = 0; i < numberOfFile; i++) {
            final int fileIndex = i;
            Future<Boolean> future = executor.submit(() -> {
                String fileName = String.format("%s/%s_data_part_%s.txt",targetClassesDirPath, sourceTable, fileIndex);
                OracleSqlLoader.Results results = OracleSqlLoader.bulkLoad(entityManagerFactory, "\"%s\"".formatted(connectionstring), destinationTable, new File(fileName));
                if (results.exitCode != OracleSqlLoader.ExitCode.SUCCESS) {
                    System.err.println("Failed. Exit code: " + results.exitCode + ". See log file: " + results.logFile);
                    return false;
                }
                return true;
            });
            futures.add(future);
        }

        // Shutdown executor service
        executor.shutdown();

        // Wait for all tasks to complete
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            System.err.println("Executor service was interrupted.");
        }

        boolean result = true;
        // Check results
        for (Future<Boolean> future : futures) {
            try {
                if (!future.get()) {
                    result = false;
                    break;
                }
            } catch (InterruptedException | ExecutionException e) {
                // Handle exception
                e.printStackTrace();
            }
        }

        boolean resultFinal = result;

        if (result) {
            resultFinal =deleteAllFromTable(sourceTable);
        }
        long time = System.currentTimeMillis() - start;
        System.out.println("sqlLoaderArchive took : " + time+ " ms");
        return resultFinal;
    }


    public boolean initData(String table,int numRows) {

        EntityManager entityManager = entityManagerFactory.createEntityManager();
        Session session = entityManager.unwrap(Session.class);

        String INIT_DATA= String.format("insert into %s select rownum, 'Name'||rownum from dual connect by rownum<=%s",table,numRows);

        AtomicBoolean success = new AtomicBoolean(Boolean.TRUE);

        session.doWork(connection -> {
            try {
                long start = System.currentTimeMillis();
                connection.setAutoCommit(false);
                Statement statement = connection.createStatement();

                // Execute the insert query
                int rowsInserted = statement.executeUpdate(INIT_DATA);


                // Commit the transaction
                connection.commit();
                // Close the resources
                statement.close();
                connection.close();
                long time = System.currentTimeMillis() - start;
                System.out.println(rowsInserted + " rows inserted into " + table+".");
                System.out.println("initData took : " + time+ " ms");


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

}
