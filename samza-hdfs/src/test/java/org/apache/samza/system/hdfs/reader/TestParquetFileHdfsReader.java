/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.samza.system.hdfs.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by hongzhengshi on 3/14/17.
 */
public class TestParquetFileHdfsReader {

    private static final String WORKING_DIRECTORY = TestParquetFileHdfsReader.class.getResource("/reader").getPath();
    private static final String PARQUET_FILE = WORKING_DIRECTORY + "/strings-2.parquet";
    private ParquetFileHdfsReader preader;
    private SystemStreamPartition ssp;

    @Before
    public void setUp() throws Exception {
        ssp = new SystemStreamPartition("hdfs", "testStream", new Partition(0));
        preader = new ParquetFileHdfsReader(ssp);
    }

    @Test
    public void testIterate() throws Exception {
        preader.open(PARQUET_FILE, "0");

        String[] expected = {"text: hello world\n", "text: hello foo\n"};
        List<String> results = new ArrayList<>();
        IncomingMessageEnvelope ev;
        while (preader.hasNext()) {
            ev = preader.readNext();
            results.add(ev.getMessage().toString());
        }
        assertArrayEquals(expected, results.toArray());
        preader.close();
    }

    @Test
    public void testOffset() throws Exception {
        preader.open(PARQUET_FILE, "1");

        String[] expected = {"text: hello foo\n"};
        List<String> results = new ArrayList<>();
        IncomingMessageEnvelope ev;
        while (preader.hasNext()) {
            ev = preader.readNext();
            results.add(ev.getMessage().toString());
        }
        assertArrayEquals(expected, results.toArray());
        preader.close();
    }

    @Test
    public void testSeek() throws Exception {
        preader.open(PARQUET_FILE, "0");
        preader.seek("1");

        String[] expected = {"text: hello foo\n"};
        List<String> results = new ArrayList<>();
        IncomingMessageEnvelope ev;
        while (preader.hasNext()) {
            ev = preader.readNext();
            results.add(ev.getMessage().toString());
        }
        assertArrayEquals(expected, results.toArray());
        preader.close();
    }

    @Test(expected = NoSuchElementException.class)
    public void testNextThrowsExceptionWhenNoMoreElement() {
        preader.open(PARQUET_FILE, "0");
        while (true) {
            preader.readNext();
        }
    }

}