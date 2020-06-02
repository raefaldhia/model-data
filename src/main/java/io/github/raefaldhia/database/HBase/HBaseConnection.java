package io.github.raefaldhia.database.HBase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

public class HBaseConnection {
    private final org.apache.hadoop.hbase.client.Connection
    rawInstance;

    public HBaseConnection (final org.apache.hadoop.hbase.client.Connection rawInstance) {
        this.rawInstance = rawInstance;
    }

    void close () throws IOException {
        rawInstance.close();
    }

    public static interface getTableCallback {
        void execute(final Table table) throws IOException;
    }

    public Admin getAdmin() throws IOException {
	return rawInstance.getAdmin();
    }
    public void getTable(TableName tableName, getTableCallback callback) throws IOException {
        final Table
        table = rawInstance.getTable(tableName);

        callback.execute(table);

        table.close();
    }

    private static final Configuration 
    config = HBaseConfiguration.create();

    public static interface CreateCallback {
        void execute(HBaseConnection connection) throws IOException;
    }

    public static void Create (CreateCallback callback) throws IOException {
        final HBaseConnection
        connection = new HBaseConnection(ConnectionFactory.createConnection(config));

        callback.execute(connection);

        connection.close();
    }
}
