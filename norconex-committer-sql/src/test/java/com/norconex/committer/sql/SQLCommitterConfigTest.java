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

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.commons.lang3.ClassUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

import com.norconex.commons.lang.config.XMLConfigurationUtil;
import com.norconex.commons.lang.encrypt.EncryptionKey;
import com.norconex.commons.lang.encrypt.EncryptionKey.Source;
import com.norconex.commons.lang.log.CountingConsoleAppender;

public class SQLCommitterConfigTest {

    @Test
    public void testWriteRead() throws Exception {
        SQLCommitter committer = new SQLCommitter();

        committer.setDriverPath("driverPath");
        committer.setDriverClass("driverClass");
        committer.setConnectionUrl("connectionUrl");
        committer.setUsername("username");
        committer.setPassword("password");
        committer.setPasswordKey(new EncryptionKey("keyValue", Source.FILE));
        committer.getProperties().setProperty("key1", "value1");
        committer.getProperties().setProperty("key2", "value2");
        committer.setTableName("tableName");
        committer.setCreateTableSQL("createTableSQL");
        committer.setCreateFieldSQL("createFieldSQL");
        committer.setMultiValuesJoiner("^");
        committer.setFixFieldNames(true);
        committer.setFixFieldValues(true);
        
        committer.setQueueDir("my-queue-dir");
        committer.setSourceContentField("sourceContentField");
        committer.setTargetContentField("targetContentField");
        committer.setSourceReferenceField("idField");
        committer.setKeepSourceContentField(true);
        committer.setKeepSourceReferenceField(false);
        committer.setQueueSize(10);
        committer.setCommitBatchSize(1);

        System.out.println("Writing/Reading this: " + committer);
        XMLConfigurationUtil.assertWriteRead(committer);
    }

    
    @Test
    public void testValidation() throws IOException {
        CountingConsoleAppender appender = new CountingConsoleAppender();
        appender.startCountingFor(XMLConfigurationUtil.class, Level.WARN);
        try (Reader r = new InputStreamReader(getClass().getResourceAsStream(
                ClassUtils.getShortClassName(getClass()) + ".xml"))) {
            XMLConfigurationUtil.newInstance(r);
        } finally {
            appender.stopCountingFor(XMLConfigurationUtil.class);
        }
        Assert.assertEquals("Validation warnings/errors were found.", 
                0, appender.getCount());
    }
}
