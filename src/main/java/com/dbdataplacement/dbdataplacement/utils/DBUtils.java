package com.dbdataplacement.dbdataplacement.utils;

import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Transactional
public class DBUtils {
    private final  EntityManagerFactory entityManagerFactory;

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


    public boolean createTable(String table) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        Session session = entityManager.unwrap(Session.class);
        AtomicBoolean success = new AtomicBoolean(Boolean.TRUE);

        session.doWork(connection -> {
            try {
                Statement statement = connection.createStatement();

                // Drop the table if it exists
                String dropQuery = String.format("DROP TABLE %s", table);
                statement.executeUpdate(dropQuery);

                // Create the table
                String createQuery = String.format("CREATE TABLE %s (id NUMBER, name VARCHAR2(50), age NUMBER, email VARCHAR2(100))", table);
                statement.executeUpdate(createQuery);

                // Close the resources
                statement.close();
                System.out.println("Table " + table + " created successfully.");
            } catch (Exception e) {
                e.printStackTrace();
                success.set(Boolean.FALSE);
            }
        });

        return success.get();
    }

    public boolean insertData(String table, int numRows) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        Session session = entityManager.unwrap(Session.class);
        AtomicBoolean success = new AtomicBoolean(Boolean.TRUE);

        session.doWork(connection -> {
            try {
                Statement statement = connection.createStatement();

                // Insert fake data
                String insertQuery = String.format("INSERT INTO %s SELECT ROWNUM, 'Name' || ROWNUM, FLOOR(DBMS_RANDOM.VALUE(18, 80)), 'email' || ROWNUM || '@example.com' FROM dual CONNECT BY ROWNUM <= %d", table, numRows);
                int rowsInserted = statement.executeUpdate(insertQuery);

                // Close the resources
                statement.close();
                System.out.println(rowsInserted + " rows inserted into " + table + ".");
            } catch (Exception e) {
                e.printStackTrace();
                success.set(Boolean.FALSE);
            }
        });

        return success.get();
    }

    public int getRowCount(String table) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        Session session = entityManager.unwrap(Session.class);
        AtomicInteger rowCount = new AtomicInteger(0);

        session.doWork(connection -> {
            try {
                Statement statement = connection.createStatement();
                String countQuery = String.format("SELECT COUNT(*) FROM %s", table);
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
    public Map<String, Object> generateTableStatistics(String table) {
        EntityManager entityManager = entityManagerFactory.createEntityManager();
        Session session = entityManager.unwrap(Session.class);
        Map<String, Object> statistics = new HashMap<>();

        session.doWork(connection -> {
            try {
                Statement statement = connection.createStatement();

                // Get the number of rows
                String countQuery = String.format("SELECT COUNT(*) FROM %s", table);
                ResultSet countResult = statement.executeQuery(countQuery);
                if (countResult.next()) {
                    statistics.put("row_count", countResult.getInt(1));
                }

                // Get the average age
                String avgAgeQuery = String.format("SELECT AVG(age) FROM %s", table);
                ResultSet avgAgeResult = statement.executeQuery(avgAgeQuery);
                if (avgAgeResult.next()) {
                    statistics.put("average_age", avgAgeResult.getDouble(1));
                }

                // Get the minimum age
                String minAgeQuery = String.format("SELECT MIN(age) FROM %s", table);
                ResultSet minAgeResult = statement.executeQuery(minAgeQuery);
                if (minAgeResult.next()) {
                    statistics.put("min_age", minAgeResult.getInt(1));
                }

                // Get the maximum age
                String maxAgeQuery = String.format("SELECT MAX(age) FROM %s", table);
                ResultSet maxAgeResult = statement.executeQuery(maxAgeQuery);
                if (maxAgeResult.next()) {
                    statistics.put("max_age", maxAgeResult.getInt(1));
                }

                // Close the resources
                countResult.close();
                avgAgeResult.close();
                minAgeResult.close();
                maxAgeResult.close();
                statement.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        return statistics;
    }

}
