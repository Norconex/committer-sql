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

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.text.StrSubstitutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.committer.core.AbstractCommitter;
import com.norconex.committer.core.AbstractMappedCommitter;
import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.IDeleteOperation;
import com.norconex.commons.lang.encrypt.EncryptionKey;
import com.norconex.commons.lang.encrypt.EncryptionUtil;
import com.norconex.commons.lang.time.DurationParser;
import com.norconex.commons.lang.xml.EnhancedXMLStreamWriter;

/**
 * <p>
 * Commit documents to a SQL database.
 * </p>
 * 
 * <h3>Handling of missing table/fields</h3>
 * <p>
 * By default, this Committer will throw an exception when trying to insert
 * values into non-existing database table or fields. It is recommended your 
 * make sure your database table exists and the document fields being sent 
 * to the committer match your database fields.
 * </p>
 * <p>
 * Alternatively, you can provide the necessary SQLs to create a new
 * table as well as new fields as needed using 
 * {@link #setCreateTableSQL(String)} and {@link #setCreateFieldSQL(String)}
 * respectively. Make sure to use the following placeholder variables 
 * as needed in the provided SQL(s) to have them automatically replaced by 
 * this Committer.
 * </p>
 * 
 * <dl>
 *   
 *   <dt>${tableName}</dt>
 *   <dd>
 *     Your table name, to be replaced with the value supplied with
 *     {@link #setTableName(String)}. 
 *   </dd>
 *   
 *   <dt>${targetReferenceField}</dt>
 *   <dd>
 *     The field that will hold your document reference. This usually is
 *     your table primary key. Default is {@value #DEFAULT_SQL_ID_FIELD} and
 *     can be overwritten with {@link #setTargetReferenceField(String)}.
 *   </dd>
 *   
 *   <dt>${targetContentField}</dt>
 *   <dd>
 *     The field that will hold your document content (or "body"). 
 *     Default is {@value #DEFAULT_SQL_CONTENT_FIELD} and can be
 *     overwritten with {@link #setTargetContentField(String)}.
 *   </dd>
 *   
 *   <dt>${fieldName}</dt>
 *   <dd>
 *     A field name to be created if you provided an SQL for creating new 
 *     fields.
 *   </dd>
 *   
 * </dl>
 * 
 * <h3>Authentication</h3>
 * <p>
 * For databases requiring authentication, the <code>password</code> can 
 * optionally be encrypted using {@link EncryptionUtil} 
 * (or command-line "encrypt.bat" or "encrypt.sh").
 * In order for the password to be decrypted properly, you need
 * to specify the encryption key used to encrypt it. The key can be stored
 * in a few supported locations and a combination of
 * <code>passwordKey</code>
 * and <code>passwordKeySource</code> must be specified to properly
 * locate the key. The supported sources are:
 * </p>
 * <table border="1" summary="">
 *   <tr>
 *     <th><code>passwordKeySource</code></th>
 *     <th><code>passwordKey</code></th>
 *   </tr>
 *   <tr>
 *     <td><code>key</code></td>
 *     <td>The actual encryption key.</td>
 *   </tr>
 *   <tr>
 *     <td><code>file</code></td>
 *     <td>Path to a file containing the encryption key.</td>
 *   </tr>
 *   <tr>
 *     <td><code>environment</code></td>
 *     <td>Name of an environment variable containing the key.</td>
 *   </tr>
 *   <tr>
 *     <td><code>property</code></td>
 *     <td>Name of a JVM system property containing the key.</td>
 *   </tr>
 * </table>
 * 
 * <h3>XML configuration usage:</h3>
 * <pre>
 *  &lt;committer class="com.norconex.committer.sql.SQLCommitter"&gt;
 *      &lt;!-- Mandatory settings --&gt;
 *      &lt;driverClass&gt;
 *        (Class name of the JDBC driver to use.)
 *      &lt;/driverClass&gt;
 *      &lt;connectionUrl&gt;
 *        (JDBC connection URL.)
 *      &lt;/connectionUrl&gt;
 *      &lt;tableName&gt;
 *        (The target database table name where documents will be committed.)
 *      &lt;/tableName&gt;
 *  
 *      &lt;!-- Other settings --&gt;
 *      &lt;driverPath&gt;
 *        (Path to JDBC driver. Not required if already in classpath.)
 *      &lt;/driverPath&gt;
 *      &lt;properties&gt;
 *          &lt;property key="(property name)"&gt;(Property value.)&lt;/property&gt;
 *          &lt;!-- You can have multiple property. --&gt;
 *      &lt;/properties&gt;
 *      
 *      &lt;createTableSQL&gt;
 *          &lt;!-- 
 *            The CREATE statement used to create a table if it does not 
 *            already exist. If you need fields of specific types,
 *            specify them here.  The following variables are expected
 *            and will be replaced with the configuration options of the same name:
 *            ${tableName}, ${targetReferenceField} and ${targetContentField}.
 *            Example:
 *            --&gt;
 *          CREATE TABLE ${tableName} (
 *              ${targetReferenceField} VARCHAR(32672) NOT NULL, 
 *              ${targetContentField}  CLOB, 
 *              PRIMARY KEY ( ${targetReferenceField} ),
 *              title VARCHAR(256)
 *          )
 *      &lt;/createTableSQL&gt;
 *      &lt;createFieldSQL&gt;
 *          &lt;!-- 
 *            The ALTER statement used to create missing table fields.  
 *            The ${tableName} variable and will be replaced with 
 *            the configuration option of the same name. The ${fieldName} 
 *            variable will be replaced by newly encountered field names.
 *            Example:
 *            --&gt;
 *          ALTER TABLE ${tableName} ADD ${fieldName} VARCHAR(32672)
 *      &lt;/createFieldSQL&gt;
 *      &lt;multiValuesJoiner&gt;
 *          (One or more characters to join multi-value fields.
 *           Default is "|".)
 *      &lt;/multiValuesJoiner&gt;
 *      &lt;fixFieldNames&gt;
 *          (Attempts to prevent insertion errors by converting characters that
 *           are not underscores or alphanumeric to underscores.
 *           Will also remove all non alphabetic characters that begins
 *           a field name.)
 *      &lt;/fixFieldNames&gt;
 *      &lt;fixFieldValues&gt;
 *          (Attempts to prevent insertion errors by truncating values
 *           that are larger than their defined maximum field length.)
 *      &lt;/fixFieldValues&gt;
 *            
 *      &lt;!-- Use the following if authentication is required. --&gt;
 *      &lt;username&gt;(Optional user name)&lt;/username&gt;
 *      &lt;password&gt;(Optional user password)&lt;/password&gt;
 *      &lt;!-- Use the following if password is encrypted. --&gt;
 *      &lt;passwordKey&gt;(the encryption key or a reference to it)&lt;/passwordKey&gt;
 *      &lt;passwordKeySource&gt;[key|file|environment|property]&lt;/passwordKeySource&gt;
 *      
 *      &lt;sourceReferenceField keep="[false|true]"&gt;
 *         (Optional name of field that contains the document reference, when 
 *          the default document reference is not used.  
 *          Once re-mapped, this metadata source field is 
 *          deleted, unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceReferenceField&gt;
 *      &lt;targetReferenceField&gt;
 *         (Name of the database target field where the store a document unique 
 *          identifier (sourceReferenceField).  If not specified, 
 *          default is "id". Typically is a tableName primary key.) 
 *      &lt;/targetReferenceField&gt;
 *      &lt;sourceContentField keep="[false|true]"&gt;
 *         (If you wish to use a metadata field to act as the document 
 *          "content", you can specify that field here.  Default 
 *          does not take a metadata field but rather the document content.
 *          Once re-mapped, the metadata source field is deleted,
 *          unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceContentField&gt;
 *      &lt;targetContentField&gt;
 *         (Target repository field name for a document content/body.
 *          Default is "content". Since document content can sometimes be 
 *          quite large, a CLOB field is usually best advised.)
 *      &lt;/targetContentField&gt;
 *      &lt;commitBatchSize&gt;
 *         (Max number of documents to send to the database at once.)
 *      &lt;/commitBatchSize&gt;
 *      &lt;queueDir&gt;(optional path where to queue files)&lt;/queueDir&gt;
 *      &lt;queueSize&gt;(max queue size before committing)&lt;/queueSize&gt;
 *      &lt;maxRetries&gt;(max retries upon commit failures)&lt;/maxRetries&gt;
 *      &lt;maxRetryWait&gt;(max delay in milliseconds between retries)&lt;/maxRetryWait&gt;
 *  &lt;/committer&gt;
 * </pre>
 * <p>
 * XML configuration entries expecting millisecond durations
 * can be provided in human-readable format (English only), as per 
 * {@link DurationParser} (e.g., "5 minutes and 30 seconds" or "5m30s").
 * </p>
 * 
 * <h4>Usage example:</h4>
 * <p>
 * The following example uses an H2 database and creates the table and fields 
 * as they are encountered, storing all new fields as VARCHAR, making sure
 * those new fields are no longer than 5000 characters. 
 * </p> 
 * <pre>
 *  &lt;committer class="com.norconex.committer.sql.SQLCommitter"&gt;
 *      &lt;driverPath&gt;/path/to/driver/h2.jar&lt;/driverPath&gt;
 *      &lt;driverClass&gt;org.h2.Driver&lt;/driverClass&gt;
 *      &lt;connectionUrl&gt;jdbc:h2:file:///path/to/db/h2&lt;/connectionUrl&gt;
 *      &lt;tableName&gt;test_table&lt;/tableName&gt;
 *      &lt;createTableSQL&gt;
 *          CREATE TABLE ${tableName} (
 *              ${targetReferenceField} VARCHAR(32672) NOT NULL, 
 *              ${targetContentField}  CLOB, 
 *              PRIMARY KEY ( ${targetReferenceField} )
 *          )
 *      &lt;/createTableSQL&gt;
 *      &lt;createFieldSQL&gt;
 *          ALTER TABLE ${tableName} ADD ${fieldName} VARCHAR(5000)
 *      &lt;/createFieldSQL&gt;
 *      &lt;fixFieldValues&gt;true&lt;/fixFieldValues&gt;
 *  &lt;/committer&gt;
 * </pre>
 *  
 * @author Pascal Essiembre
 */
public class SQLCommitter extends AbstractMappedCommitter {

    private static final Logger LOG = LogManager.getLogger(SQLCommitter.class);

    /** Default SQL primary key field */
    public static final String DEFAULT_SQL_ID_FIELD = "id";
    /** Default SQL content field */
    public static final String DEFAULT_SQL_CONTENT_FIELD = "content";
    /** Default multi-value join string */
    public static final String DEFAULT_MULTI_VALUES_JOINER = "|";

    private static final String[] NO_REFLECT_FIELDS = new String[] {
            "existingFields", "tableVerified", "datasource"
    }; 
    
    private String driverPath;
    private String driverClass;
    private String connectionUrl;
    private String username;
    private String password;
    private EncryptionKey passwordKey;
    private final Properties properties = new Properties();

    private String tableName;
    private String createTableSQL;
    private String createFieldSQL;
    
    private boolean fixFieldNames;
    private boolean fixFieldValues;
    private String multiValuesJoiner = DEFAULT_MULTI_VALUES_JOINER;
    
    // When we create missing ones... so we do not check if exists each time.
    // key = field name; value = field size
    private final Map<String, Integer> existingFields = new HashMap<>();
    // If we could confirm whether the tableName exists
    private boolean tableVerified;
    private BasicDataSource datasource;
    
    
    /**
     * Constructor.
     */
    public SQLCommitter() {
        super();
        setTargetReferenceField(DEFAULT_SQL_ID_FIELD);
        setTargetContentField(DEFAULT_SQL_CONTENT_FIELD);
    }

    public String getDriverPath() {
        return driverPath;
    }
    public void setDriverPath(String driverPath) {
        this.driverPath = driverPath;
    }

    public String getDriverClass() {
        return driverClass;
    }
    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }
    public void setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
    }

    public String getUsername() {
        return username;
    }
    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }
    public void setPassword(String password) {
        this.password = password;
    }

    public EncryptionKey getPasswordKey() {
        return passwordKey;
    }
    public void setPasswordKey(EncryptionKey passwordKey) {
        this.passwordKey = passwordKey;
    }

    public Properties getProperties() {
        return properties;
    }

    public String getTableName() {
        return tableName;
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getCreateTableSQL() {
        return createTableSQL;
    }
    public void setCreateTableSQL(String createTableSQL) {
        this.createTableSQL = createTableSQL;
    }
    
    public String getCreateFieldSQL() {
        return createFieldSQL;
    }
    public void setCreateFieldSQL(String createFieldSQL) {
        this.createFieldSQL = createFieldSQL;
    }

    public String getMultiValuesJoiner() {
        return multiValuesJoiner;
    }
    public void setMultiValuesJoiner(String multiValuesJoiner) {
        this.multiValuesJoiner = multiValuesJoiner;
    }

    public boolean isFixFieldNames() {
        return fixFieldNames;
    }
    public void setFixFieldNames(boolean fixFieldNames) {
        this.fixFieldNames = fixFieldNames;
    }

    public boolean isFixFieldValues() {
        return fixFieldValues;
    }
    public void setFixFieldValues(boolean fixFieldValues) {
        this.fixFieldValues = fixFieldValues;
    }

    @Override
    public void commit() {
        super.commit();
        closeIfDone();
    }

    //TODO The following is a workaround to not having
    // a close() method (or equivalent) on the Committers yet.
    // So we check that the caller is not itself, which means it should
    // be the parent framework, which should in theory, call this only 
    // once. This is safe to do as the worst case scenario is that a new
    // client is re-created.
    // Remove this method once proper init/close is added to Committers
    private void closeIfDone() {
        StackTraceElement[] els = Thread.currentThread().getStackTrace();
        for (StackTraceElement el : els) {
            if (AbstractCommitter.class.getName().equals(el.getClassName())
                    && "commitIfReady".equals(el.getMethodName())) {
                return;
            }
        }
        close();
    }

    public synchronized void close() {
        if (datasource != null) {
            try {
                datasource.close();
            } catch (SQLException e) {
                throw new CommitterException("Could not close datasource.", e);
            }
        }
    }
    
    @Override
    protected void commitBatch(List<ICommitOperation> batch) {
        LOG.info("Sending " + batch.size() 
                + " commit operations to SQL database.");
        try {
            QueryRunner q = new QueryRunner(nullSafeDataSource());
            ensureTable(q);
            for (ICommitOperation op : batch) {
                if (op instanceof IAddOperation) {
                    addOperation((IAddOperation) op, q);
                } else if (op instanceof IDeleteOperation) {
                    deleteOperation((IDeleteOperation) op, q); 
                } else {
                    close();
                    throw new CommitterException("Unsupported operation:" + op);
                }
            }
            LOG.info("Done sending commit operations to database.");
        } catch (CommitterException e) {
            close();
            throw e;
        } catch (Exception e) {
            close();
            throw new CommitterException(
                    "Could not commit batch to database.", e);
        }
    }

    private void addOperation(IAddOperation add, QueryRunner q)
            throws SQLException {
        String docId = add.getMetadata().getString(getTargetReferenceField());
        if (StringUtils.isBlank(docId)) {
            docId = add.getReference();
        }
        
        List<String> fields = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (Entry<String, List<String>> entry : add.getMetadata().entrySet()) {
            String field = entry.getKey();
            String value = StringUtils.join(
                    entry.getValue(), getMultiValuesJoiner());
            fields.add(fixFieldName(field));
            values.add(value);
        }

        String sql = "INSERT INTO " + tableName + "("
                + StringUtils.join(fields, ",")
                + ") VALUES (" + StringUtils.repeat("?", ", ", values.size())
                + ")";
        if (LOG.isTraceEnabled()) {
            LOG.trace("SQL: " + sql);
        }
        sqlInsertDoc(q, sql, docId, fields, values);
    }

    private void deleteOperation(IDeleteOperation del, QueryRunner q)
            throws SQLException {
        runDelete(q, del.getReference());
    }

    private void sqlInsertDoc(QueryRunner q, String sql, String docId, 
            List<String> fields, List<String> values) throws SQLException {
        ensureFields(q, fields);
        Object[] args = new Object[values.size()];
        int i = 0;
        for (String value : values) {
            args[i] = fixFieldValue(fields.get(i), value);
            i++;
        }

        // If it already exists, delete it first.
        if (recordExists(q, docId)) {
            LOG.debug("Record exists. Deleting it first (" + docId + ").");
            runDelete(q, docId);
        }
        q.update(sql, args);
    }

    private String fixFieldName(String fieldName) {
        if (!fixFieldNames) {
            return fieldName;
        }
        String newName = fieldName.replaceAll("\\W+", "_");
        newName = newName.replaceFirst("^[\\d_]+", "");
        if (LOG.isDebugEnabled() && !newName.equals(fieldName)) {
            LOG.debug("Field name modified: " + fieldName + " -> " + newName);
        }
        return newName;
    }
    
    private String fixFieldValue(String fieldName, String value) {
        if (!fixFieldValues) {
            return value;
        }
        Integer size = existingFields.get(
                StringUtils.lowerCase(fieldName, Locale.ENGLISH));
        if (size == null) {
            return value;
        }
        String newValue = StringUtils.truncate(value, size);
        if (LOG.isDebugEnabled() && !newValue.equals(value)) {
            LOG.debug("Value truncated: " + value + " -> " + newValue);
        }
        return newValue;
    }
    
    //--- Verifying/creating tables/fields -------------------------------------

    private boolean tableExists(QueryRunner q) {
        try {
            // for table existence, we cannot rely enough on return value
            runExists(q, null);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }
    private boolean recordExists(QueryRunner q, String docId)
            throws SQLException {
        return runExists(q, getTargetReferenceField() + " = ?", docId);
    }
    private boolean runExists(QueryRunner q, String where, Object... values)
            throws SQLException {
        String sql = "SELECT 1 FROM " + tableName;
        if (StringUtils.isNotBlank(where)) {
            sql += " WHERE " + where;
        }
        LOG.debug(sql);
        Integer val = (Integer) q.query(sql, new ScalarHandler<>(), values);
        return val != null && val == 1;
    }
    private void runDelete(QueryRunner q, String docId) throws SQLException {
        String deleteSQL = "DELETE FROM " + tableName
                + " WHERE " + getTargetReferenceField() + " = ?";
        LOG.trace(deleteSQL);
        q.update(deleteSQL, docId);
    }
    
    private synchronized void ensureTable(QueryRunner q) throws SQLException {
        // if table was verified or no CREATE statement specified,
        // return right away.
        if (tableVerified || StringUtils.isBlank(createTableSQL)) {
            return;
        }
        LOG.info("Checking if table \"" + tableName + "\" exists...");
        if (!tableExists(q)) {
            LOG.info("Table \"" + tableName + "\" does not exist. "
                    + "Attempting to create it...");
            String sql = interpolate(getCreateTableSQL(), null);
            LOG.debug(sql);
            q.update(sql);
            LOG.info("Table created.");
        } else {
            LOG.info("Table \"" + tableName + "\" exists.");
        }
        
        loadFieldsMetadata(q);
        
        tableVerified = true;
    }

    private synchronized void ensureFields(QueryRunner q, List<String> fields)
            throws SQLException {
        // If not SQL to create field,  we assume they should all exist.
        if (StringUtils.isBlank(getCreateFieldSQL())) {
            return;
        }

        Set<String> currentFields = existingFields.keySet();
        boolean hasNew = false;
        for (String field : fields) {
            if (!currentFields.contains(
                    StringUtils.lowerCase(field, Locale.ENGLISH))) {
                // Create field
                createField(q, field);
                hasNew = true;
            }
        }
        
        // Update fields metadata
        if (hasNew) {
            loadFieldsMetadata(q);
        }
    }
    
    private void createField(QueryRunner q, String field) throws SQLException {
        try {
            String sql = interpolate(getCreateFieldSQL(), field);
            LOG.trace(sql);
            q.update(sql);
            LOG.info("New field \"" + field + "\" created.");
        } catch (SQLException e) {
              LOG.info("New field \"" + field + "\" could not be created.");
              throw e;
        }
    }

    private void loadFieldsMetadata(QueryRunner q) throws SQLException {
        // Add existing field info
        q.query("SELECT * FROM " + tableName, new ResultSetHandler<Void>(){
            @Override
            public Void handle(ResultSet rs) throws SQLException {
                ResultSetMetaData metadata = rs.getMetaData();
                for (int i = 1; i <= metadata.getColumnCount(); i++) {
                    existingFields.put(StringUtils.lowerCase(
                            metadata.getColumnLabel(i), Locale.ENGLISH),
                            metadata.getColumnDisplaySize(i));
                }
                return null;
            }
        });
    }
    
    private String interpolate(String text, String fieldName) {
        Map<String, String> vars = new HashMap<>();
        vars.put("tableName", getTableName());
        vars.put("targetReferenceField", getTargetReferenceField());
        vars.put("targetContentField", getTargetContentField());
        if (StringUtils.isNotBlank(fieldName)) {
            vars.put("fieldName", fieldName);
        }
        return StrSubstitutor.replace(text, vars);
    }
    
    private synchronized BasicDataSource nullSafeDataSource() {
        if (datasource == null) {
            if (StringUtils.isBlank(getDriverClass())) {
                throw new CommitterException("No driver class specified.");
            }
            if (StringUtils.isBlank(getConnectionUrl())) {
                throw new CommitterException("No connection URL specified.");
            }
            if (StringUtils.isBlank(getTableName())) {
                throw new CommitterException("No table name specified.");
            }
            BasicDataSource ds = new BasicDataSource();
            // if path is blank, we assume it is already in classpath
            if (StringUtils.isNotBlank(driverPath)) {
                try {
                    ds.setDriverClassLoader(new URLClassLoader(
                            new URL[] { new File(driverPath).toURI().toURL() }, 
                            getClass().getClassLoader()));
                } catch (MalformedURLException e) {
                    throw new CommitterException(
                            "Invalid driver path: " + driverPath, e);
                }
            }
            ds.setDriverClassName(driverClass);
            ds.setUrl(connectionUrl);
            ds.setDefaultAutoCommit(true);
            ds.setUsername(username);
            ds.setPassword(EncryptionUtil.decrypt(
                    getPassword(), getPasswordKey()));
            for (String key : properties.stringPropertyNames()) {
                ds.addConnectionProperty(key, properties.getProperty(key));
            }
            datasource = ds;
        }
        return datasource;
    }    

    @Override
    protected void saveToXML(XMLStreamWriter writer) throws XMLStreamException {
        EnhancedXMLStreamWriter w = new EnhancedXMLStreamWriter(writer);
        w.writeElementString("driverPath", getDriverPath());
        w.writeElementString("driverClass", getDriverClass());
        w.writeElementString("connectionUrl", getConnectionUrl());
        w.writeElementString("username", getUsername());
        w.writeElementString("password", getPassword());
        if (!properties.isEmpty()) {
            w.writeStartElement("properties");
            for (Entry<Object, Object> e : properties.entrySet()) {
                w.writeStartElement("property");
                w.writeAttributeString(
                        "key", Objects.toString(e.getKey(), ""));
                w.writeCharacters(Objects.toString(e.getValue(), ""));
                w.writeEndElement();
            }
            w.writeEndElement();
        }
        w.writeElementString("tableName", getTableName());
        w.writeElementString("createTableSQL", getCreateTableSQL());
        w.writeElementString("createFieldSQL", getCreateFieldSQL());
        w.writeElementBoolean("fixFieldNames", isFixFieldNames());
        w.writeElementBoolean("fixFieldValues", isFixFieldValues());
        w.writeElementString("multiValuesJoiner", getMultiValuesJoiner());
        
        // Encrypted password:
        EncryptionKey key = getPasswordKey();
        if (key != null) {
            w.writeElementString("passwordKey", key.getValue());
            if (key.getSource() != null) {
                w.writeElementString("passwordKeySource",
                        key.getSource().name().toLowerCase());
            }
        }
    }

    @Override
    protected void loadFromXml(XMLConfiguration xml) {
        setDriverPath(xml.getString("driverPath", getDriverPath()));
        setDriverClass(xml.getString("driverClass", getDriverClass()));
        setConnectionUrl(xml.getString("connectionUrl", getConnectionUrl()));
        setUsername(xml.getString("username", getUsername()));
        setPassword(xml.getString("password", getPassword()));
        List<HierarchicalConfiguration> xmlProps = 
                xml.configurationsAt("properties.property");
        if (!xmlProps.isEmpty()) {
            properties.clear();
            for (HierarchicalConfiguration xmlProp : xmlProps) {
                properties.setProperty(
                        xmlProp.getString("[@key]"), xmlProp.getString(""));
            }
        }
        setTableName(xml.getString("tableName", getTableName()));
        setCreateTableSQL(xml.getString("createTableSQL", getCreateTableSQL()));
        setCreateFieldSQL(xml.getString("createFieldSQL", getCreateFieldSQL()));
        setFixFieldNames(xml.getBoolean("fixFieldNames", isFixFieldNames()));
        setFixFieldValues(xml.getBoolean("fixFieldValues", isFixFieldValues()));
        setMultiValuesJoiner(xml.getString(
                "multiValuesJoiner", getMultiValuesJoiner()));
        
        // encrypted password:
        String xmlKey = xml.getString("passwordKey", null);
        String xmlSource = xml.getString("passwordKeySource", null);
        if (StringUtils.isNotBlank(xmlKey)) {
            EncryptionKey.Source source = null;
            if (StringUtils.isNotBlank(xmlSource)) {
                source = EncryptionKey.Source.valueOf(xmlSource.toUpperCase());
            }
            setPasswordKey(new EncryptionKey(xmlKey, source));
        }
    }
    
    @Override
    public boolean equals(final Object other) {
        return EqualsBuilder.reflectionEquals(this, other, NO_REFLECT_FIELDS);
    }
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, NO_REFLECT_FIELDS);
    }
    @Override
    public String toString() {
        return new ReflectionToStringBuilder(this, 
                ToStringStyle.SHORT_PREFIX_STYLE).setExcludeFieldNames(
                        NO_REFLECT_FIELDS).toString();
    }
}
