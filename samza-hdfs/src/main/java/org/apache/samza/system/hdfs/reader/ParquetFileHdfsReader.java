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

import java.lang.Integer;
import java.io.IOException;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.hadoop.fs.Path;
import org.apache.samza.SamzaException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of the HdfsReader that reads and processes parquet format
 * files.
 */
public class ParquetFileHdfsReader implements SingleFileHdfsReader {

    private static final Logger LOG = LoggerFactory.getLogger(ParquetFileHdfsReader.class);
    private int offset;
    private Group lastGroup;

    private final SystemStreamPartition systemStreamPartition;
    private ParquetReader<Group> parquetReader;


    public ParquetFileHdfsReader(SystemStreamPartition systemStreamPartition) {
        this.systemStreamPartition = systemStreamPartition;
        this.parquetReader = null;
    }

    @Override
    public void open(String pathStr, String singleFileOffset) {
        LOG.info(String.format("%s: Open file [%s] with file offset [%s] for read", systemStreamPartition, pathStr, singleFileOffset));
        Path path = new Path(pathStr);
        try {
            parquetReader = ParquetReader.builder(new GroupReadSupport(), path).build();
            seek(singleFileOffset);
        } catch (IOException e) {
            throw new SamzaException(e);
        }
    }

    @Override
    public void seek(String singleFileOffset) {
        int bootstrapOffset = Integer.parseInt(singleFileOffset);
        offset = 0;
        // TODO: seek updates internal state lastGroup singleFileOffset times, could be avoid...
        for (int i = 0; i < bootstrapOffset; i++) {
            if (!hasNext()) break;
        }
    }

    @Override
    public IncomingMessageEnvelope readNext() {
        return new IncomingMessageEnvelope(systemStreamPartition, Integer.toString(offset++), null, lastGroup);
    }

    @Override
    public boolean hasNext() {
        try {
            Group group = parquetReader.read();
            if (group == null) {
                return false;
            }
            lastGroup = group;
            return true;
        } catch (IOException e) {
            throw new SamzaException(e);
        }
    }

    @Override
    public void close() {
        LOG.info("About to close file reader for " + systemStreamPartition);
        try {
            parquetReader.close();
        } catch (IOException e) {
            throw new SamzaException(e);
        }
        LOG.info("File reader closed for " + systemStreamPartition);
    }

    @Override
    public String nextOffset() {
        return Integer.toString(offset);
    }
}
