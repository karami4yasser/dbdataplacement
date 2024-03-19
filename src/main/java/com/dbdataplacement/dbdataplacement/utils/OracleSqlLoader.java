package com.dbdataplacement.dbdataplacement.utils;

import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class OracleSqlLoader {
    private static final String OUTPUT_FILE_PREFIX = "data_part_";
    private static final String OUTPUT_FILE_SUFFIX = ".txt";
    private static int MAX_ROWS_PER_FILE ;

    @Autowired
    public OracleSqlLoader(@Value("${max_rows_per_file}") int max) {
        MAX_ROWS_PER_FILE = max;
    }


        public enum ExitCode {SUCCESS, FAIL, WARN, FATAL, UNKNOWN}

        public static class Results {
            public final ExitCode exitCode;
            public final File controlFile;
            public final File logFile;
            public final File badFile;
            public final File discardFile;

            public Results(ExitCode exitCode, File controlFile, File logFile, File badFile, File discardFile) {
                this.exitCode = exitCode;
                this.controlFile = controlFile;
                this.logFile = logFile;
                this.badFile = badFile;
                this.discardFile = discardFile;
            }
        }

        public static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("win");

        /**
         * Helper method. Get list of table columns, to be inserted in control file.
         * TSV data file must match this column order.
         */
        public static List<String> getTableColumns(final EntityManagerFactory entityManagerFactory, final String tableName) throws SQLException {
            EntityManager entityManager = entityManagerFactory.createEntityManager();
            Session session = entityManager.unwrap(Session.class);
            final List<String> ret = new ArrayList<>();

            session.doWork(conn -> {
                PreparedStatement ps = conn.prepareStatement("select COLUMN_NAME from USER_TAB_COLUMNS where TABLE_NAME = ? order by COLUMN_ID");
                ps.setObject(1, tableName);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ret.add(rs.getString(1));
                    }
                }
            });
            return ret;
        }

        /**
         * Helper method. Generate intermediate control file.
         */
        public static String createControlFile(
                final String dataFileName,
                final String badFileName,
                final String discardFileName,
                final String tableName,
                final List<String> columnNames
        ) {
            return "" +
                    "load data infile '" + dataFileName + "'\n" +
                    "badfile '" + badFileName + "'\n" +
                    "discardfile '" + discardFileName + "'\n" +
                    "append\n" +
                    "into table " + tableName + "\n" +
                    "fields terminated by '\\t'\n" + // TAB separated fields
                    columnNames.toString().replace("[", "( ").replace("]", " )\n");
        }

        /**
         * Run SQL*Loader process.
         */
        public static ExitCode runSqlLdrProcess(
                final File initialDir,
                final String stdoutLogFile,
                final String stderrLogFile,
                final String controlFile,
                final String logFile,
                final String connectionString
        ) throws IOException {
            System.out.println("Loading in DB : " + controlFile);
            final ProcessBuilder pb = new ProcessBuilder(
                    "sqlldr",
                    "userid="+connectionString,
                    "control=" +controlFile,
                    "log=" + logFile,
                    "silent=header",
                    "PARALLEL=TRUE"
            );
            pb.directory(initialDir);
            if (stdoutLogFile != null) pb.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(initialDir, stdoutLogFile)));
            if (stderrLogFile != null) pb.redirectError(ProcessBuilder.Redirect.appendTo(new File(initialDir, stderrLogFile)));
            final Process process = pb.start();
            try {
                process.waitFor(); // TODO may implement here timeout mechanism and progress monitor instead of just blocking the caller thread.
            } catch (InterruptedException ignored) {
            }

            final int exitCode = process.exitValue();

            // Exit codes are OS dependent. Convert them to our OS independent.
            // See: https://docs.oracle.com/cd/B19306_01/server.102/b14215/ldr_params.htm#i1005019
            switch (exitCode) {
                case 0:
                    return ExitCode.SUCCESS;
                case 1:
                    return ExitCode.FAIL;
                case 2:
                    return ExitCode.WARN;
                case 3:
                    return IS_WINDOWS ? ExitCode.FAIL : ExitCode.FATAL;
                case 4:
                    return ExitCode.FATAL;
                default:
                    return ExitCode.UNKNOWN;
            }
        }

    /**
     *
     * @param entityManagerFactory
     * @param connectionString
     * @param tableName
     * @param dataFile
     * @return
     * @throws IOException
     * @throws SQLException
     */
        public static Results bulkLoad(
                final EntityManagerFactory entityManagerFactory,
                final String connectionString,
                final String tableName,
                final File dataFile
        ) throws IOException, SQLException {

            final File initialDirectory = dataFile.getParentFile();
            final String dataFileName = dataFile.getName();
            final String controlFileName = dataFileName + ".ctl";
            final String logFileName = dataFileName + ".log";
            final String badFileName = dataFileName + ".bad";
            final String discardFileName = dataFileName + ".discard";

            final File controlFile = new File(initialDirectory, controlFileName);
            controlFile.delete();
            final List<String> columnNames = getTableColumns(entityManagerFactory, tableName);
            final String controlFileContents = createControlFile(dataFileName, badFileName, discardFileName, tableName, columnNames);
            Files.write(controlFile.toPath(), controlFileContents.getBytes(), StandardOpenOption.CREATE_NEW);

            final ExitCode exitCode = runSqlLdrProcess(
                    initialDirectory,
                    dataFileName + ".stdout.log",
                    dataFileName + ".stderr.log",
                    controlFileName,
                    logFileName,
                    connectionString
            );

            // Return to the caller names of files generated inside this method.
            Results ret = new Results(
                    exitCode,
                    controlFile,
                    new File(initialDirectory, logFileName),
                    new File(initialDirectory, badFileName),
                    new File(initialDirectory, discardFileName)
            );
            return ret;
        }

    /**
     *
     * @param entityManagerFactory
     * @param tableName
     * @param outputDirectory
     * @return
     * @throws SQLException
     * @throws IOException
     */
    public static synchronized int exportDataToSqlLoaderFiles(EntityManagerFactory entityManagerFactory, String tableName, String outputDirectory) throws SQLException, IOException {

        EntityManager entityManager = entityManagerFactory.createEntityManager();
        AtomicInteger fileCount = new AtomicInteger(0);
        AtomicInteger rowCount = new AtomicInteger(0);
        Session session = entityManager.unwrap(Session.class);


        session.doWork(conn -> {

            try {
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);

                BufferedWriter writer = null;
                while (rs.next()) {
                    if (rowCount.get() % MAX_ROWS_PER_FILE == 0) {
                        if (writer != null) {
                            writer.close();
                        }
                        writer = new BufferedWriter(new FileWriter(outputDirectory + "/" +tableName+"_"+ OUTPUT_FILE_PREFIX + fileCount.get() + OUTPUT_FILE_SUFFIX));
                        fileCount.addAndGet(1);
                    }
                    // Writing data to file
                    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                        assert writer != null;
                        writer.write(rs.getString(i));
                        if (i < rs.getMetaData().getColumnCount()) {
                            writer.write("\t"); // Assuming tab-separated format
                        }
                    }
                    assert writer != null;
                    writer.newLine();
                    rowCount.addAndGet(1);
                }
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        return fileCount.get();
    }

}
