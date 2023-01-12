package org.apache.seatunnel.connector.selectdb.sink.committer;

import java.util.Objects;

public class SelectDBCommittable {
    private final String hostPort;
    private final String clusterName;
    private final String copySQL;

    public SelectDBCommittable(String hostPort, String clusterName, String copySQL) {
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
        SelectDBCommittable that = (SelectDBCommittable) o;
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
