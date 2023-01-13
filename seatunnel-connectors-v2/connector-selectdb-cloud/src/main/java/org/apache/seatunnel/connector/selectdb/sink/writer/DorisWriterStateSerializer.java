/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connector.selectdb.sink.writer;

import org.apache.seatunnel.api.serialization.Serializer;

import java.io.*;

/**
 * Serializer for SelectDBWriterState.
 */
public class DorisWriterStateSerializer implements Serializer<SelectDBWriterState> {


    @Override
    public byte[] serialize(SelectDBWriterState obj) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(obj.getLabelPrefix());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public SelectDBWriterState deserialize(byte[] serialized) throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             final DataInputStream in = new DataInputStream(bais)) {
            final String labelPrefix = in.readUTF();
            return new SelectDBWriterState(labelPrefix);
        }
    }
}
