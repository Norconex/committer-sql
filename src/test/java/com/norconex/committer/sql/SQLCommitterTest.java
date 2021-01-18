/* Copyright 2017-2021 Norconex Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.norconex.committer.sql;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.IOUtils.toInputStream;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.io.input.NullInputStream;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.norconex.committer.core3.CommitterContext;
import com.norconex.committer.core3.CommitterException;
import com.norconex.committer.core3.DeleteRequest;
import com.norconex.committer.core3.UpsertRequest;
import com.norconex.commons.lang.TimeIdGenerator;
import com.norconex.commons.lang.map.Properties;

class SQLCommitterTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(SQLCommitterTest.class);

    private static final String TEST_ID = "3";
    private static final String TEST_CONTENT = "This is test content.";
    private static final String TEST_TABLE = "test_table";
    private static final String TEST_FLD_PK = "ID";
    private static final String TEST_FLD_CONTENT = "CONTENT";
    private static final String DRIVER_CLASS = "org.h2.Driver";

    private static String connectionURL;

    @TempDir
    File tempDir;

    @BeforeEach
    void beforeEach() {
        LOG.debug("Creating new database.");
        connectionURL = "jdbc:h2:"
              + tempDir.getAbsolutePath()
              + ";WRITE_DELAY=0;AUTOCOMMIT=ON;AUTO_SERVER=TRUE";
        try {
            withinDbSession(qr -> qr.update("DROP TABLE " + TEST_TABLE));
        } catch (SQLException e) {
            // OK if not found.
        }
    }

    @Test
    void testCommitAdd() throws Exception {
        // Add new doc to SQL table
        withinCommitterSession(c -> {
            c.upsert(upsertRequest(TEST_ID, TEST_CONTENT));
        });
        List<Map<String, Object>> docs = getAllDocs();
        assertEquals(1, docs.size());
        assertTestDoc(docs.get(0));
    }

    @Test
    void testAddWithQueueContaining2documents() throws Exception{
        withinCommitterSession(c -> {
            c.upsert(upsertRequest("1", "Document 1"));
            c.upsert(upsertRequest("2", "Document 2"));
        });

        //Check that there is 2 documents in SQL table
        Assertions.assertEquals(2, getAllDocs().size());
    }

    @Test
    void testCommitQueueWith3AddCommandAnd1DeleteCommand()
            throws Exception{
        withinCommitterSession(c -> {
            c.upsert(upsertRequest("1", "Document 1"));
            c.upsert(upsertRequest("2", "Document 2"));
            c.delete(new DeleteRequest("1", new Properties()));
            c.upsert(upsertRequest("3", "Document 3"));
        });

        //Check that there are 2 documents in SQL table
        Assertions.assertEquals(2, getAllDocs().size());
    }

    @Test
    void testCommitQueueWith3AddCommandAnd2DeleteCommand()
            throws Exception{
        withinCommitterSession(c -> {
            c.upsert(upsertRequest("1", "Document 1"));
            c.upsert(upsertRequest("2", "Document 2"));
            c.delete(new DeleteRequest("1", new Properties()));
            c.delete(new DeleteRequest("2", new Properties()));
            c.upsert(upsertRequest("3", "Document 3"));
        });

        //Check that there is 1 documents in SQL table
        Assertions.assertEquals(1, getAllDocs().size());
    }

    @Test
    void testCommitDelete() throws Exception {

        // Add a document
        withinCommitterSession(c -> {
            c.upsert(upsertRequest("1", "Document 1"));
        });

        // Delete it in a new session.
        withinCommitterSession(c -> {
            c.delete(new DeleteRequest("1", new Properties()));
        });

        // Check that it's remove from SQL table
        Assertions.assertEquals(0, getAllDocs().size());
    }


    @Test
    void testMultiValueFields() throws Exception {
        Properties metadata = new Properties();
        String fieldname = "MULTI";
        metadata.set(fieldname, "1", "2", "3");

        withinCommitterSession(c -> {
            c.getConfig().setMultiValuesJoiner("_");
            c.upsert(upsertRequest(TEST_ID, null, metadata));
        });

        // Check that it's in SQL table
        List<Map<String, Object>> docs = getAllDocs();
        assertEquals(1, docs.size());
        Map<String, Object> doc = docs.get(0);

        // Check multi values are still there
        assertEquals(3, doc.get(fieldname).toString().split("_").length,
                "Multi-value not saved properly.");
    }

    @Test
    void testFixFieldValues() throws Exception {
        withinCommitterSession(c -> {
            Properties metadata = new Properties();
            metadata.set("LONGSINGLE", StringUtils.repeat("a", 50));
            metadata.set("LONGMULTI",
                    StringUtils.repeat("a", 10),
                    StringUtils.repeat("b", 10),
                    StringUtils.repeat("c", 10),
                    StringUtils.repeat("d", 10));

            SQLCommitterConfig config = c.getConfig();
            config.setFixFieldValues(true);
            config.setMultiValuesJoiner("-");
            c.upsert(upsertRequest(TEST_ID, null, metadata));
        });


        List<Map<String, Object>> docs = getAllDocs();
        assertEquals(1, docs.size());

        Map<String, Object> doc = docs.get(0);
        assertEquals(TEST_ID, doc.get(TEST_FLD_PK));
        // Check values were truncated
        assertEquals(doc.get("LONGSINGLE"), StringUtils.repeat("a", 30));
        assertEquals(doc.get("LONGMULTI"),
                StringUtils.repeat("a", 10) + "-"
              + StringUtils.repeat("b", 10) + "-"
              + StringUtils.repeat("c", 8));
    }

    @Test
    void testFixFieldNames() throws Exception {
        withinCommitterSession(c -> {
            Properties metadata = new Properties();
            metadata.set("A$B&C %E_F", "test1");
            metadata.set("99FIELD2", "test2");
            metadata.set("*FIELD3", "test3");

            SQLCommitterConfig config = c.getConfig();
            config.setFixFieldNames(true);
            c.upsert(upsertRequest(TEST_ID, null, metadata));
        });

        List<Map<String, Object>> docs = getAllDocs();
        assertEquals(1, docs.size());

        Map<String, Object> doc = docs.get(0);
        assertEquals(TEST_ID, doc.get(TEST_FLD_PK));

        // Check values were truncated
        assertEquals(doc.get("A_B_C_E_F"), "test1");
        assertEquals(doc.get("FIELD2"), "test2");
        assertEquals(doc.get("FIELD3"), "test3");
    }


    private UpsertRequest upsertRequest(String id, String content) {
        return upsertRequest(id, content, null);
    }
    private UpsertRequest upsertRequest(
            String id, String content, Properties metadata) {
        Properties p = metadata == null ? new Properties() : metadata;
        return new UpsertRequest(id, p, content == null
                ? new NullInputStream(0) : toInputStream(content, UTF_8));
    }

    private void assertTestDoc(Map<String, Object> doc) {
        assertEquals(TEST_ID, doc.get(TEST_FLD_PK));
        assertEquals(TEST_CONTENT, doc.get(TEST_FLD_CONTENT));
    }

    private SQLCommitter createSQLCommitter()
            throws CommitterException {
        CommitterContext ctx = CommitterContext.builder()
                .setWorkDir(new File(tempDir,
                        "work-" + TimeIdGenerator.next()).toPath()).build();
        SQLCommitter committer = new SQLCommitter();
        SQLCommitterConfig config = committer.getConfig();
        config.setConnectionUrl(connectionURL);
        config.setDriverClass(DRIVER_CLASS);
        config.setConnectionUrl(connectionURL);
        config.setTableName(TEST_TABLE);
        config.setPrimaryKey(TEST_FLD_PK);
        config.setCreateTableSQL(
                "CREATE TABLE {tableName} ("
              + "  {primaryKey} VARCHAR(32672) NOT NULL, "
              + "  content CLOB, "
              + "  PRIMARY KEY ({primaryKey}) "
              + ")");
        config.setCreateFieldSQL(
                "ALTER TABLE {tableName} ADD {fieldName} VARCHAR(30)");
        committer.init(ctx);
        return committer;
    }

    private SQLCommitter withinCommitterSession(CommitterConsumer c)
            throws CommitterException {
        SQLCommitter committer = createSQLCommitter();
        try {
            c.accept(committer);
        } catch (CommitterException e) {
            throw e;
        } catch (Exception e) {
            throw new CommitterException(e);
        }
        committer.close();
        return committer;
    }

    private <T> T withinDbSession(DbFunction<T> c) throws SQLException {
        BasicDataSource datasource = new BasicDataSource();
        datasource.setDriverClassName(DRIVER_CLASS);
        datasource.setUrl(connectionURL);
        datasource.setDefaultAutoCommit(true);
        T t = c.apply(new QueryRunner(datasource));
        datasource.close();
        return t;
    }

    @FunctionalInterface
    private interface CommitterConsumer {
        void accept(SQLCommitter c) throws Exception;
    }
    @FunctionalInterface
    private interface DbFunction<T> {
        T apply(QueryRunner q) throws SQLException;
    }

    private List<Map<String, Object>> getAllDocs() throws SQLException {
        return withinDbSession(qr -> {
            return qr.query("SELECT * FROM " + TEST_TABLE,
                    new MapListHandler(new ClobAwareRowProcessor()));
        });
    }

    class ClobAwareRowProcessor extends BasicRowProcessor {
        @Override
        public Map<String, Object> toMap(ResultSet resultSet)
                throws SQLException {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int columnCount = resultSetMetaData.getColumnCount();
            Map<String, Object> map = new HashMap<>();
            for (int index = 1; index <= columnCount; ++index) {
                String columnName = resultSetMetaData.getColumnName(index);
                Object object = resultSet.getObject(index);
                if (object instanceof Clob) {
                    object = resultSet.getString(index);
                }
                map.put(columnName, object);
            }
            return map;
        }
    }
}
