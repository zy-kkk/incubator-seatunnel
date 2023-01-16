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

package org.apache.seatunnel.connector.selectdb.sink.committer;

import java.io.Serializable;
import java.util.Objects;

public class SelectDBCommitInfo implements Serializable {
    private final String hostPort;
    private final String clusterName;
    private final String copySQL;

    public SelectDBCommitInfo(String hostPort, String clusterName, String copySQL) {
        this.hostPort = hostPort;
        this.clusterName = clusterName;
        this.copySQL = copySQL;
    }

    public String getHostPort() {
        return hostPort;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getCopySQL() {
        return copySQL;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SelectDBCommitInfo that = (SelectDBCommitInfo) o;
        return hostPort.equals(that.hostPort) &&
                clusterName.equals(that.clusterName) &&
                copySQL.equals(that.copySQL);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostPort, clusterName, copySQL);
    }

    @Override
    public String toString() {
        return "SelectDBCommittable{" +
                "hostPort='" + hostPort + '\'' +
                ", clusterName='" + clusterName + '\'' +
                ", copySQL='" + copySQL + '\'' +
                '}';
    }
}
