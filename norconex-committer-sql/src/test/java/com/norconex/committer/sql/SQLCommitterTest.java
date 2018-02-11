/* Copyright 2017-2018 Norconex Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.norconex.commons.lang.map.Properties;

public class SQLCommitterTest {

    private static final Logger LOG = 
            LogManager.getLogger(SQLCommitterTest.class);

    @ClassRule
    public static TemporaryFolder tempDBFolder = new TemporaryFolder();
    @Rule
    public TemporaryFolder tempCommitterFolder = new TemporaryFolder();

    private static final String TEST_ID = "1";
    private static final String TEST_CONTENT = "This is test content.";
    private static final String TEST_TABLE = "test_table";
    private static final String ID_FIELD = 
            SQLCommitter.DEFAULT_SQL_ID_FIELD;
    private static final String CONTENT_FIELD = 
            SQLCommitter.DEFAULT_SQL_CONTENT_FIELD;
    private static final String DRIVER_CLASS = "org.h2.Driver";

    private static String connectionURL;
    private static BasicDataSource datasource;
    
    private SQLCommitter committer;
    private File queue;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        LOG.debug("Creating new database.");
        connectionURL = "jdbc:h2:"
                + tempDBFolder.newFolder("h2").getAbsolutePath()
                + ";WRITE_DELAY=0;AUTOCOMMIT=ON;AUTO_SERVER=TRUE";
        
        datasource = new BasicDataSource();
        datasource.setDriverClassName(DRIVER_CLASS);
        datasource.setUrl(connectionURL);
        datasource.setDefaultAutoCommit(true);
    }
    
    @Before
    public void setup() throws Exception {
        LOG.debug("Creating new committer and cleaning table.");
        queue = tempCommitterFolder.newFolder("queue");
        committer = new SQLCommitter();
        committer.setQueueDir(queue.toString());
        committer.setDriverClass(DRIVER_CLASS);
        committer.setConnectionUrl(connectionURL);
        committer.setTableName(TEST_TABLE);
        
        // to force committing single operations:
        committer.setQueueSize(1);

        // defaults, tests can overwrite
        committer.setCreateTableSQL(
                "CREATE TABLE ${tableName} ("
              + "  ${targetReferenceField} VARCHAR(32672) NOT NULL, "
              + "  ${targetContentField}  CLOB, "
              + "  PRIMARY KEY (${targetReferenceField}) "
              + ")");
        committer.setCreateFieldSQL(
                "ALTER TABLE ${tableName} ADD ${fieldName} VARCHAR(30)");
        
        try {
            getQueryRunner().update("DROP TABLE " + TEST_TABLE);
        } catch (SQLException e) {
            // OK if not found.
        }
    }
    
    @After
    public void tearDown() throws IOException, SQLException {
        LOG.debug("Closing committer.");
        committer.close();
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        LOG.debug("Closing database.");
        datasource.close();
    }
    
    @Test
    public void testCommitAdd() throws Exception {
        // Add new doc
        try (InputStream is = getContentStream()) {
            committer.add(TEST_ID, is, new Properties());
            committer.commit();
        }
        Properties doc = getDocument(TEST_ID); 
        assertTrue("Not found.", isFound(doc));
        assertTrue("Bad content.", hasTestContent(doc));
    }

    @Test
    public void testCommitDelete() throws Exception {
        // Add the document to be deleted
        try (InputStream is = getContentStream()) {
            committer.add(TEST_ID, is, new Properties());
        }
        
        assertTrue("Not properly added.", isFound(getDocument(TEST_ID)));
        
        // Queue it to be deleted
        committer.remove(TEST_ID, new Properties());
        committer.commit();

        // Check that it's removed from database
        assertFalse("Was not deleted.", isFound(getDocument(TEST_ID)));
    }
    
    @Test
    public void testRemoveQueuedFilesAfterAdd() throws Exception {
        // Add new doc to database
        committer.add(TEST_ID, new NullInputStream(0), new Properties());
        committer.commit();

        // After commit, make sure queue is emptied of all files
        assertTrue(FileUtils.listFiles(queue, null, true).isEmpty());
    }

    @Test
    public void testRemoveQueuedFilesAfterDelete() throws Exception {
        // Add new doc to database
        committer.remove(TEST_ID, new Properties());
        committer.commit();

        // After commit, make sure queue is emptied of all files
        assertTrue(FileUtils.listFiles(queue, null, true).isEmpty());
    }

    @Test
    public void testSetSourceReferenceField() throws Exception {
        // Force to use a reference field instead of the default
        // reference ID.
        String sourceReferenceField = "customId";
        committer.setSourceReferenceField(sourceReferenceField);
        Properties metadata = new Properties();
        String customIdValue = "ABC";
        metadata.setString(sourceReferenceField, customIdValue);

        // Add new doc to database with a difference id than the one we
        // assigned in source reference field
        try (InputStream is = getContentStream()) {
            committer.add(TEST_ID, is, metadata);
            committer.commit();
        }

        // Check that it's in database using the custom ID
        Properties doc = getDocument(customIdValue); 
        assertTrue("Not found.", isFound(doc));
        assertTrue("Bad content.", hasTestContent(doc));
        assertTrue("sourceReferenceField was saved.", 
                StringUtils.isBlank(doc.getString(sourceReferenceField)));
    }
    
    @Test
    public void testKeepIdSourceField() throws Exception {
        // Force to use a reference field instead of the default
        // reference ID.
        String sourceReferenceField = "customId";
        committer.setSourceReferenceField(sourceReferenceField);
        Properties metadata = new Properties();
        String customIdValue = "ABC";
        metadata.setString(sourceReferenceField, customIdValue);

        // Add new doc to database with a difference id than the one we
        // assigned in source reference field. Set to keep that 
        // field.
        committer.setKeepSourceReferenceField(true);
        try (InputStream is = getContentStream()) {
            committer.add(TEST_ID, is, metadata);
            committer.commit();
        }

        // Check that it's in database using the custom ID
        Properties doc = getDocument(customIdValue); 
        assertTrue("Not found.", isFound(doc));
        assertTrue("Bad content.", hasTestContent(doc));
        assertTrue("sourceReferenceField was not saved.", 
                StringUtils.isNotBlank(doc.getString(sourceReferenceField)));
    }
    
    @Test
    public void testCustomSourceContentField() throws Exception {
        
        // Set content from metadata
        String sourceContentField = "customContent";
        Properties metadata = new Properties();
        metadata.setString(sourceContentField, TEST_CONTENT);
        
        // Add new doc to database. Set a null input stream, because content
        // will be taken from metadata. 
        committer.setSourceContentField(sourceContentField);
        committer.add(TEST_ID, new NullInputStream(0), metadata);
        committer.commit();
        
        // Check that it's in database
        Properties doc = getDocument(TEST_ID); 
        assertTrue("Not found.", isFound(doc));
        assertTrue("Bad content.", hasTestContent(doc));
        
        // Check custom source field is removed (default behavior)
        assertTrue("sourceContentField was saved.", 
                StringUtils.isBlank(doc.getString(sourceContentField)));
    }
    
    @Test
    public void testKeepCustomSourceContentField() throws Exception {
        // Set content from metadata
        String sourceContentField = "customContent";
        Properties metadata = new Properties();
        metadata.setString(sourceContentField, TEST_CONTENT);
        
        // Add new doc to database. Set a null input stream, because content
        // will be taken from metadata. Set to keep the source metadata
        // field.
        committer.setSourceContentField(sourceContentField);
        committer.setKeepSourceContentField(true);
        committer.add(TEST_ID, new NullInputStream(0), metadata);
        committer.commit();
        
        // Check that it's in database
        Properties doc = getDocument(TEST_ID); 
        assertTrue("Not found.", isFound(doc));
        assertTrue("Bad content.", hasTestContent(doc));
        
        // Check custom source field is kept
        assertTrue("sourceContentField was not saved.", 
                StringUtils.isNotBlank(doc.getString(sourceContentField)));
    }
    
    @Test
    public void testCustomTargetContentField() throws Exception {
        String targetContentField = "customContent";
        Properties metadata = new Properties();
        metadata.setString(targetContentField, TEST_CONTENT);
        
        // Add new doc to database
        committer.setTargetContentField(targetContentField);
        try (InputStream is = getContentStream()) {
            committer.add(TEST_ID, is, metadata);
            committer.commit();
        }
        
        // Check that it's in database
        Properties doc = getDocument(TEST_ID); 
        assertTrue("Not found.", isFound(doc));
        
        // Check content is available in custom content target field and
        // not in the default field
        assertEquals("targetContentField was not saved.", TEST_CONTENT, 
                doc.getString(targetContentField));
        assertTrue("Default content field was saved.",
                StringUtils.isBlank(doc.getString(CONTENT_FIELD)));
    }
    
    @Test
	public void testMultiValueFields() throws Exception {
    	Properties metadata = new Properties();
        String fieldname = "multi";
		metadata.setString(fieldname, "1", "2", "3");
        
        committer.setMultiValuesJoiner("^");
        committer.add(TEST_ID, new NullInputStream(0), metadata);
        committer.commit();
        
        // Check that it's in database
        Properties doc = getDocument(TEST_ID); 
        assertTrue("Not found.", isFound(doc));
        
        // Check multi values are still there
        assertEquals("Multi-value not saved properly.", 3, 
                StringUtils.split(doc.getString(fieldname), "^").length);
	}

    @Test
    public void testFixFieldValues() throws Exception {
        Properties metadata = new Properties();
        metadata.setString("longsingle", StringUtils.repeat("a", 50));
        metadata.setString("longmulti", 
                StringUtils.repeat("a", 10),
                StringUtils.repeat("b", 10),
                StringUtils.repeat("c", 10),
                StringUtils.repeat("d", 10));
        
        committer.setFixFieldValues(true);
        committer.setMultiValuesJoiner("-");
        committer.add(TEST_ID, new NullInputStream(0), metadata);
        committer.commit();
        
        // Check that it's in database
        Properties doc = getDocument(TEST_ID); 
        assertTrue("Not found.", isFound(doc));

        
        // Check values were truncated
        assertEquals(doc.getString("longsingle"), StringUtils.repeat("a", 30));
        assertEquals(doc.getString("longmulti"), 
                StringUtils.repeat("a", 10) + "-"
              + StringUtils.repeat("b", 10) + "-"
              + StringUtils.repeat("c", 8));
    }

    @Test
    public void testFixFieldNames() throws Exception {
        Properties metadata = new Properties();
        metadata.setString("A$b&c %e_f", "test1");
        metadata.setString("99field2", "test2");
        metadata.setString("*field3", "test3");
        
        committer.setFixFieldNames(true);
        committer.add(TEST_ID, new NullInputStream(0), metadata);
        committer.commit();
        
        // Check that it's in database
        Properties doc = getDocument(TEST_ID); 
        assertTrue("Not found.", isFound(doc));

        
        // Check values were truncated
        assertEquals(doc.getString("a_b_c_e_f"), "test1");
        assertEquals(doc.getString("field2"), "test2");
        assertEquals(doc.getString("field3"), "test3");
    }
    
    private boolean hasTestContent(Properties doc) throws IOException {
        return TEST_CONTENT.equals(getContent(doc));
    }
    private String getContent(Properties doc) throws IOException {
        return doc.getString(CONTENT_FIELD);
    }
    private boolean isFound(Properties doc) throws IOException {
        return !doc.isEmpty();
    }
    private Properties getDocument(String id) throws SQLException {
        Properties doc = new Properties(true);
        doc.load(getQueryRunner().query(
                "SELECT * FROM " + TEST_TABLE + " WHERE " + ID_FIELD + " = ?",
                new MapHandler(new ClobAwareRowProcessor()), id));
        return doc;
    }
    
    private static QueryRunner getQueryRunner() {
        return new QueryRunner(datasource);
    }
    private static InputStream getContentStream() throws IOException {
        return IOUtils.toInputStream(TEST_CONTENT, StandardCharsets.UTF_8);
    }
    
    public class ClobAwareRowProcessor extends BasicRowProcessor {
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
