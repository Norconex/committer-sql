/* Copyright 2017 Norconex Inc.
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
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;

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
 * values into non-existing database fields or table.
 * Make sure your database table exists and the document fields 
 * being sent to the committer match your database fields.  Alternatively,
 * you can tell the committer to attempt creating a missing table or missing
 * fields with {@link #setCreateMissing(boolean)}.  The document content
 * will be created as a <code>CLOB</code> database data type.  All other
 * fields created will be <code>VARCHAR(32672)</code>.  
 * If you want to have the table automatically created but define the fields
 * yourself, you can provide your own <code>CREATE</code> SQL with
 * {@link #setCreateTableSQL(String)}.  When using this options, make sure
 * your SQL create a table of the same name as {@link #setTableName(String)}.
 * </p>
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
 *      &lt;toUppercase&gt;
 *        (Default will send all table and field names as lowercase.
 *         Set to <code>true</code> to send as uppercase.)
 *      &lt;/toUppercase&gt;
 *      &lt;createMissing&gt;
 *        (Create missing table if not found. Default is <code>false</code>.)
 *      &lt;/createMissing&gt;
 *      &lt;createTableSQL&gt;
 *        (Optional SQL for creating missing table if not found. 
 *         Default uses a predefined SQL.)
 *      &lt;/createTableSQL&gt;
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
 *         the default document reference is not used.  
 *         Once re-mapped, this metadata source field is 
 *         deleted, unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceReferenceField&gt;
 *      &lt;targetReferenceField&gt;
 *         (Name of the database target field where the store a document unique 
 *         identifier (sourceReferenceField).  If not specified, 
 *         default is "id". Typically is a tableName primary key.) 
 *      &lt;/targetReferenceField&gt;
 *      &lt;sourceContentField keep="[false|true]"&gt;
 *         (If you wish to use a metadata field to act as the document 
 *         "content", you can specify that field here.  Default 
 *         does not take a metadata field but rather the document content.
 *         Once re-mapped, the metadata source field is deleted,
 *         unless "keep" is set to <code>true</code>.)
 *      &lt;/sourceContentField&gt;
 *      &lt;targetContentField&gt;
 *         (Target repository field name for a document content/body.
 *          Default is "content". Since document content can sometimes be 
 *          quite large, a CLOB field is usually best advised.)
 *      &lt;/targetContentField&gt;
 *      &lt;commitBatchSize&gt;
 *          (Max number of documents to send to the database at once.)
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
 * The following example uses the minimum required settings:.  
 * </p> 
 * <pre>
 *  &lt;committer class="com.norconex.committer.sql.SQLCommitter"&gt;
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
            "existingFields", "tableVerified", "tableExists", 
            "multiValuesJoiner", "datasource"
    }; 
    
    private String driverPath;
    private String driverClass;
    private String connectionUrl;
    private String username;
    private String password;
    private EncryptionKey passwordKey;
    private final Properties properties = new Properties();
    private String tableName;
    private boolean createMissing;
    private boolean toUppercase;
    private String createTableSQL;
    
    // When we create missing ones... so we do not check if exists each time.
    private final List<String> existingFields = new ArrayList<>();
    // If we could confirm whether the tableName exists
    private boolean tableVerified;
    private boolean tableExists;
    private String multiValuesJoiner;
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

    public boolean isCreateMissing() {
        return createMissing;
    }
    public void setCreateMissing(boolean createMissing) {
        this.createMissing = createMissing;
    }

    public boolean isToUppercase() {
        return toUppercase;
    }
    public void setToUppercase(boolean toUppercase) {
        this.toUppercase = toUppercase;
    }

    public String getCreateTableSQL() {
        return createTableSQL;
    }
    public void setCreateTableSQL(String createTableSQL) {
        this.createTableSQL = createTableSQL;
    }

    public String getMultiValuesJoiner() {
        return multiValuesJoiner;
    }
    public void setMultiValuesJoiner(String multiValuesJoiner) {
        this.multiValuesJoiner = multiValuesJoiner;
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
            throws IOException, SQLException {
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
            fields.add(adjustCase(field));
            values.add(value);
        }

        String sql = "INSERT INTO " + adjustCase(tableName) + "("
                + StringUtils.join(fields, ",")
                + ") VALUES (" + StringUtils.repeat("?", ", ", values.size())
                + ")";
        if (LOG.isTraceEnabled()) {
            LOG.trace("SQL: " + sql);
        }
        sqlInsertDoc(q, sql, docId, fields, values.toArray());
    }

    private void deleteOperation(IDeleteOperation del, QueryRunner q)
            throws IOException, SQLException {
        String deleteSQL = "DELETE FROM " + adjustCase(tableName)
                + " WHERE " + adjustCase(getTargetReferenceField())
                + " = ?";
        LOG.trace(deleteSQL);
        q.update(deleteSQL, del.getReference());
    }
    
    private void sqlInsertDoc(QueryRunner q, String sql, String docId, 
            List<String> fields, Object[] values) throws SQLException {
        try {
            // If it already exists, delete it first.
            String selectSQL = "SELECT 1 FROM " + adjustCase(tableName) + " WHERE "
                    + adjustCase(getTargetReferenceField()) + " = ?";
            LOG.trace(selectSQL);
            if (q.query(selectSQL, 
                    new ScalarHandler<Object>(), docId) != null) {
                LOG.debug("Record exists. Deleting it first (" + docId + ").");
                String deleteSQL = "DELETE FROM " + adjustCase(tableName)
                        + " WHERE " + adjustCase(getTargetReferenceField())
                        + " = ?";
                LOG.trace(deleteSQL);
                q.update(deleteSQL, docId);
            }
            q.update(sql, values);
        } catch (SQLException e) {
            if ((!tableVerified && tableCreatedIfMissing(q))
                    || fieldsCreatedIfMissing(q, fields)) {
                // missing tableName or fields created, try again 
                sqlInsertDoc(q, sql, docId, fields, values);
            } else {
                throw e;
            }
        }
    }

    private synchronized boolean fieldsCreatedIfMissing(
            QueryRunner q, List<String> fields) throws SQLException {
        if (!tableExists || !createMissing) {
            return false;
        }
        
        final List<String> tableFields = new ArrayList<>();
        q.query("SELECT * FROM " + adjustCase(tableName), 
                new ResultSetHandler<Void>(){
            @Override
            public Void handle(ResultSet rs) throws SQLException {
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    String tableField = rs.getMetaData().getColumnLabel(i);
                    tableFields.add(adjustCase(tableField));
                }
                return null;
            }
        });
        
        boolean atLeastOneCreated = false;
        for (String field : fields) {
            if (existingFields.contains(field)) {
                continue;
            }
            if (!tableFields.contains(field)) {
                try {
                    q.update("ALTER TABLE " + adjustCase(tableName) + " ADD "
                            + field + " VARCHAR(32672)");
                    atLeastOneCreated = true;
                    LOG.info("Missing tableName field \"" + field + "\" created.");
                } catch (SQLException e) {
                    LOG.info("Missing tableName field \"" + field 
                            + "\" could not be created.", e);
                    throw e;
                }
            }
            existingFields.add(adjustCase(field));
        }
        return atLeastOneCreated;
    }
    
    private synchronized boolean tableCreatedIfMissing(QueryRunner q) {
        if (tableVerified) {
            // if it was verify and we are getting here again, it means
            // it did not exist.
            return false;
        }
        tableVerified = true;
        try {
            q.query("SELECT * FROM " + tableName, new ScalarHandler<>());
            tableExists = true;
            return false;
        } catch (SQLException e) {
            if (createMissing) {
                LOG.info("Table \"" + tableName + "\" does not exist. "
                        + "Attempting to create it.");
                String pk = adjustCase(getTargetReferenceField());
                try {
                    String sql = getCreateTableSQL();
                    if (StringUtils.isBlank(sql)) {
                        sql = "CREATE TABLE " + tableName + " (" 
                                + pk + " VARCHAR(32672) NOT NULL, "
                                + getTargetContentField() + " CLOB, "
                                + "PRIMARY KEY (" + pk + "))";
                    }
                    q.update(sql);
                    tableExists = true;
                    LOG.trace(sql);
                    LOG.info("Table created.");
                    return true;
                } catch (SQLException e1) {
                    LOG.error("Could not create missing table \""
                            + tableName + "\".", e1);
                    return false;
                }
            }
            LOG.error("Table \"" + tableName + "\" does not exist. "
                    + "Consider setting \"createMissing\" to \"true\".", e);
            return false;
        }
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

    private String adjustCase(String s) {
        if (toUppercase) {
            return StringUtils.upperCase(s, Locale.ENGLISH);
        }
        return StringUtils.lowerCase(s, Locale.ENGLISH);
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
        w.writeElementBoolean("createMissing", isCreateMissing());
        w.writeElementBoolean("toUppercase", isToUppercase());
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
        setCreateMissing(xml.getBoolean("createMissing", isCreateMissing()));
        setToUppercase(xml.getBoolean("toUppercase", isToUppercase()));
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
