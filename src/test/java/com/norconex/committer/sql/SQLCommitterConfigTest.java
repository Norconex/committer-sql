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

import java.io.IOException;
import java.io.Reader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.norconex.committer.core3.batch.queue.impl.FSQueue;
import com.norconex.commons.lang.ResourceLoader;
import com.norconex.commons.lang.map.PropertyMatcher;
import com.norconex.commons.lang.text.TextMatcher;
import com.norconex.commons.lang.xml.XML;

class SQLCommitterConfigTest {

    @Test
    void testWriteRead() throws Exception {
        SQLCommitter c = new SQLCommitter();

        FSQueue q = new FSQueue();
        q.setBatchSize(10);
        q.setMaxPerFolder(5);
        c.setCommitterQueue(q);

        c.setFieldMapping("subject", "title");
        c.setFieldMapping("body", "content");

        c.getRestrictions().add(new PropertyMatcher(
                TextMatcher.basic("document.reference"),
                TextMatcher.wildcard("*.pdf")));
        c.getRestrictions().add(new PropertyMatcher(
                TextMatcher.basic("title"),
                TextMatcher.wildcard("Nah!")));

        SQLCommitterConfig cfg = c.getConfig();


        cfg.setDriverPath("driverPath");
        cfg.setDriverClass("driverClass");
        cfg.setConnectionUrl("connectionUrl");
        cfg.getCredentials().setUsername("Optimus");
        cfg.getCredentials().setPassword("Prime");
        cfg.getProperties().set("key1", "value1");
        cfg.getProperties().set("key2", "value2");
        cfg.setTableName("tableName");
        cfg.setPrimaryKey("id");
        cfg.setCreateTableSQL("createTableSQL");
        cfg.setCreateFieldSQL("createFieldSQL");
        cfg.setFixFieldNames(true);
        cfg.setFixFieldValues(true);
        cfg.setMultiValuesJoiner("^");
        cfg.setTargetContentField("targetContentField");

        XML.assertWriteRead(c, "committer");
    }


    @Test
    void testValidation() throws IOException {
        Assertions.assertDoesNotThrow(() -> {
            try (Reader r = ResourceLoader.getXmlReader(this.getClass())) {
                XML xml = XML.of(r).create();
                xml.toObjectImpl(SQLCommitter.class);
            }
        });
    }
}
