package com.app.validation;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.List;

public class KuduBqCompare {

    public static void main(String[] args) {

        final String kms = args[0];
        final String tabNm = args[1];

        // Define Kudu master addresses
        final String[] KUDU_MASTERS = {"kudu.master.address1:port", "kudu.master.address2:port", "kudu.master.address3:port"};

        // Provide table name
        final String TABLE_NAME = tabNm;

        KuduClient.KuduClientBuilder clientBuilder = new KuduClient.KuduClientBuilder(kms);

        KuduClient kuduClient = clientBuilder.build();

        try {
            // Describe the table
            describeTable(kuduClient, TABLE_NAME);

            // List 5 records
            listRecords(kuduClient, TABLE_NAME);

        } catch (KuduException e) {
            e.printStackTrace();
        } finally {
            try {
                kuduClient.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
    }

    private static void describeTable(KuduClient kuduClient, String tableName) throws KuduException {
        KuduTable table = kuduClient.openTable(tableName);
        Schema schema = table.getSchema();

        // Print table name
        System.out.println("Table Name: " + tableName);

        // Print column names and data types
        List<ColumnSchema> columns = schema.getColumns();
        for (ColumnSchema column : columns) {
            System.out.println("Column Name: " + column.getName() + ", Data Type: " + column.getType());
        }
    }

    private static void listRecords(KuduClient kuduClient, String tableName) throws KuduException {
        KuduTable table = kuduClient.openTable(tableName);
        KuduScanner scanner = kuduClient.newScannerBuilder(table).build();

        int count = 0;
        while (scanner.hasMoreRows() && count < 5) {
            RowResultIterator results = scanner.nextRows();
            while (results.hasNext() && count < 5) {
                RowResult result = results.next();
                // Process the data, for example:
                System.out.println("Record: " + result.rowToString());
                count++;
            }
        }
    }

}
