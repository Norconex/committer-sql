/* Copyright 2021 Norconex Inc.
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

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.norconex.commons.lang.collection.CollectionUtil;
import com.norconex.commons.lang.map.Properties;
import com.norconex.commons.lang.security.Credentials;
import com.norconex.commons.lang.xml.XML;

/**
 * <p>
 * SQL Committer configuration.
 * </p>
 */
public class SQLCommitterConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Default SQL content field */
    public static final String DEFAULT_SQL_CONTENT_FIELD = "content";
    /** Default multi-value join string */
    public static final String DEFAULT_MULTI_VALUES_JOINER = "|";

    /** Path to the JDBC driver JAR file. */
    private String driverPath;
    /** Fully-qualified JDBC driver class name. */
    private String driverClass;
    /** JDBC connection URL. */
    private String connectionUrl;
    /** Database authentication credentials. */
    private final Credentials credentials = new Credentials();
    /** Additional JDBC connection properties. */
    private final Properties properties = new Properties();

    /** Target table name. */
    private String tableName;
    /** Primary key column name. */
    private String primaryKey;
    /** SQL used to create the target table when it does not exist. */
    private String createTableSQL;
    /** SQL used to add a new column when a field has no matching column. */
    private String createFieldSQL;

    /** Whether to sanitize field names to match SQL identifier rules. */
    private boolean fixFieldNames;
    /** Whether to truncate field values that exceed the column length. */
    private boolean fixFieldValues;
    /** Separator used to join multi-valued fields into a single string. */
    private String multiValuesJoiner = DEFAULT_MULTI_VALUES_JOINER;

    /** SQL column name where the document content is stored. */
    private String targetContentField = DEFAULT_SQL_CONTENT_FIELD;

    /**
     * Gets the path to the JDBC driver JAR file.
     * @return driver JAR path
     */
    public String getDriverPath() {
        return driverPath;
    }
    /**
     * Sets the path to the JDBC driver JAR file.
     * @param driverPath driver JAR path
     */
    public void setDriverPath(String driverPath) {
        this.driverPath = driverPath;
    }

    /**
     * Gets the fully-qualified JDBC driver class name.
     * @return JDBC driver class name
     */
    public String getDriverClass() {
        return driverClass;
    }
    /**
     * Sets the fully-qualified JDBC driver class name.
     * @param driverClass JDBC driver class name
     */
    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    /**
     * Gets the JDBC connection URL.
     * @return JDBC connection URL
     */
    public String getConnectionUrl() {
        return connectionUrl;
    }
    /**
     * Sets the JDBC connection URL.
     * @param connectionUrl JDBC connection URL
     */
    public void setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
    }

    /**
     * Gets the database authentication credentials.
     * @return credentials
     */
    public Credentials getCredentials() {
        return credentials;
    }
    /**
     * Sets the database authentication credentials.
     * @param credentials the credentials
     */
    public void setCredentials(Credentials credentials) {
        this.credentials.copyFrom(credentials);
    }

    /**
     * Gets additional JDBC connection properties.
     * @return JDBC connection properties
     */
    public Properties getProperties() {
        return properties;
    }
    /**
     * Sets additional JDBC connection properties.
     * @param properties JDBC connection properties
     */
    public void setProperties(Properties properties) {
        CollectionUtil.setAll(this.properties, properties);
    }

    /**
     * Gets the target table name.
     * @return table name
     */
    public String getTableName() {
        return tableName;
    }
    /**
     * Sets the target table name.
     * @param tableName table name
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Gets the SQL statement used to create the target table when it does
     * not exist. Use {@code {tableName}} and {@code {primaryKey}} as
     * placeholders.
     * @return table creation SQL
     */
    public String getCreateTableSQL() {
        return createTableSQL;
    }
    /**
     * Sets the SQL statement used to create the target table when it does
     * not exist. Use {@code {tableName}} and {@code {primaryKey}} as
     * placeholders.
     * @param createTableSQL table creation SQL
     */
    public void setCreateTableSQL(String createTableSQL) {
        this.createTableSQL = createTableSQL;
    }

    /**
     * Gets the SQL statement used to add a new column when a document field
     * has no matching column. Use {@code {fieldName}} as a placeholder.
     * @return field/column creation SQL
     */
    public String getCreateFieldSQL() {
        return createFieldSQL;
    }
    /**
     * Sets the SQL statement used to add a new column when a document field
     * has no matching column. Use {@code {fieldName}} as a placeholder.
     * @param createFieldSQL field/column creation SQL
     */
    public void setCreateFieldSQL(String createFieldSQL) {
        this.createFieldSQL = createFieldSQL;
    }

    /**
     * Gets the separator used to join multi-valued fields into a single string.
     * Default is {@value #DEFAULT_MULTI_VALUES_JOINER}.
     * @return multi-values joiner string
     */
    public String getMultiValuesJoiner() {
        return multiValuesJoiner;
    }
    /**
     * Sets the separator used to join multi-valued fields into a single string.
     * @param multiValuesJoiner separator string
     */
    public void setMultiValuesJoiner(String multiValuesJoiner) {
        this.multiValuesJoiner = multiValuesJoiner;
    }

    /**
     * Gets whether to sanitize field names to conform to SQL identifier rules.
     * @return {@code true} if field names are sanitized
     */
    public boolean isFixFieldNames() {
        return fixFieldNames;
    }
    /**
     * Sets whether to sanitize field names to conform to SQL identifier rules.
     * @param fixFieldNames {@code true} to sanitize field names
     */
    public void setFixFieldNames(boolean fixFieldNames) {
        this.fixFieldNames = fixFieldNames;
    }

    /**
     * Gets whether to truncate field values that exceed the column length.
     * @return {@code true} if field values are truncated
     */
    public boolean isFixFieldValues() {
        return fixFieldValues;
    }
    /**
     * Sets whether to truncate field values that exceed the column length.
     * @param fixFieldValues {@code true} to truncate oversized values
     */
    public void setFixFieldValues(boolean fixFieldValues) {
        this.fixFieldValues = fixFieldValues;
    }

    /**
     * Gets the SQL column name where the document content is stored.
     * Default is {@value #DEFAULT_SQL_CONTENT_FIELD}.
     * @return target content column name
     */
    public String getTargetContentField() {
        return targetContentField;
    }
    /**
     * Sets the SQL column name where the document content is stored.
     * @param targetContentField target content column name
     */
    public void setTargetContentField(String targetContentField) {
        this.targetContentField = targetContentField;
    }

    /**
     * Gets the primary key column name.
     * @return primary key column name
     */
    public String getPrimaryKey() {
        return primaryKey;
    }
    /**
     * Sets the primary key column name.
     * @param primaryKey primary key column name
     */
    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    void saveToXML(XML xml) {
        xml.addElement("driverPath", getDriverPath());
        xml.addElement("driverClass", getDriverClass());
        xml.addElement("connectionUrl", getConnectionUrl());
        credentials.saveToXML(xml.addElement("credentials"));
        xml.addElementMap("properties", "property", "name", properties);
        xml.addElement("tableName", getTableName());
        xml.addElement("primaryKey", getPrimaryKey());
        xml.addElement("createTableSQL", getCreateTableSQL());
        xml.addElement("createFieldSQL", getCreateFieldSQL());
        xml.addElement("fixFieldNames", isFixFieldNames());
        xml.addElement("fixFieldValues", isFixFieldValues());
        xml.addElement("multiValuesJoiner", getMultiValuesJoiner());
        xml.addElement("targetContentField", getTargetContentField());
    }

    void loadFromXML(XML xml) {
        setDriverPath(xml.getString("driverPath", getDriverPath()));
        setDriverClass(xml.getString("driverClass", getDriverClass()));
        setConnectionUrl(xml.getString("connectionUrl", getConnectionUrl()));
        xml.ifXML("credentials", x -> x.populate(credentials));
        xml.ifXML("properties", xmlProps -> {
            properties.clear();
            xmlProps.getXMLList("property").forEach(xp -> {
                properties.add(xp.getString("@name"), xp.getString("."));
            });
        });
        setTableName(xml.getString("tableName", getTableName()));
        setPrimaryKey(xml.getString("primaryKey", getPrimaryKey()));
        setCreateTableSQL(xml.getString("createTableSQL", getCreateTableSQL()));
        setCreateFieldSQL(xml.getString("createFieldSQL", getCreateFieldSQL()));
        setFixFieldNames(xml.getBoolean("fixFieldNames", isFixFieldNames()));
        setFixFieldValues(xml.getBoolean("fixFieldValues", isFixFieldValues()));
        setMultiValuesJoiner(xml.getString(
                "multiValuesJoiner", getMultiValuesJoiner()));
        setTargetContentField(
                xml.getString("targetContentField", getTargetContentField()));
    }

    @Override
    public boolean equals(final Object other) {
        return EqualsBuilder.reflectionEquals(this, other);
    }
    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }
    @Override
    public String toString() {
        return new ReflectionToStringBuilder(
                this, ToStringStyle.SHORT_PREFIX_STYLE).toString();
    }
}
