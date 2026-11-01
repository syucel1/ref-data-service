package com.example.refdata.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/** Test helper: reads whole tables as strings so two databases can be compared row by row. */
final class TableDump {
    private TableDump() {
    }

    static final String[] ORDER_BY = {"country_code", "currency_code", "client_id", "address_id", "account_id", "ssi_id", "release_id"};

    static List<String> rows(Connection c, String table, String orderBy) throws Exception {
        List<String> out = new ArrayList<>();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT * FROM " + table + " ORDER BY " + orderBy)) {
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= md.getColumnCount(); i++) sb.append(rs.getObject(i)).append('|');
                out.add(sb.toString());
            }
        }
        return out;
    }

    static long count(Connection c, String table) throws Exception {
        return scalar(c, "SELECT COUNT(*) FROM " + table);
    }

    static long scalar(Connection c, String sql) throws Exception {
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
