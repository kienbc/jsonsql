/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vng.json.jsonsql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map.Entry;
import org.apache.log4j.Logger;

/**
 *
 * @author bckien
 */
public class JsonSql {

    private final Logger logger = Logger.getLogger(this.getClass());
    private String keyStr = "";
    private String valueStr = "";

    private static final ObjectMapper mapper = new ObjectMapper();

    public String exec(String jsonData, String sql, String tableName) {
        if (!parseData(jsonData)) {
            return null;
        }
        return process(sql, tableName);
    }

    private String process(String sql, String tableName) {
        Connection conn;
        Statement stmt;
        ResultSet rs;
        String result = null;

        try {
            // create connection
            Class.forName("org.sqlite.JDBC");
//            conn = DriverManager.getConnection("jdbc:sqlite:test.db");
            conn = DriverManager.getConnection("jdbc:sqlite:");

            // create table
            String createTableQuery = "create table `" + tableName + "`(" + keyStr + ");";
//            logger.info(createTableQuery);
            stmt = conn.createStatement();
            stmt.executeUpdate(createTableQuery);

            // insert data
            String insertQuery = "insert into `" + tableName + "` values(" + valueStr + ")";
//            logger.info(insertQuery);
            stmt.executeUpdate(insertQuery);

            // exec select query
            rs = stmt.executeQuery(sql);
            if (rs.next()) {
                result = "";
                ResultSetMetaData metadata = rs.getMetaData();
                int numCol = metadata.getColumnCount();

                for (int i = 1; i <= numCol; i++) {
                    result += "\"" + metadata.getColumnName(i) + "\":";

                    if (metadata.getColumnTypeName(i).equalsIgnoreCase("text")) {
                        result += "\"" + rs.getString(i) + "\"";
                    } else {
                        result += rs.getString(i);
                    }

                    if (i < numCol) {
                        result += ", ";
                    }
                }
            }
        } catch (ClassNotFoundException | SQLException e) {
            logger.error(e.getMessage());
            return null;
        }

        return result==null?null:"{" + result + "}";
    }

    private boolean parseData(String json) {
        try {
            JsonNode rootNode = mapper.readTree(json);
            if (parseJsonNode("", rootNode)) {
                keyStr = keyStr.substring(0, keyStr.length() - 2);
                valueStr = valueStr.substring(0, valueStr.length() - 2);
                return true;
            }
            return false;
        } catch (IOException e) {
            logger.error(e.getMessage());
            return false;
        }
    }

    private boolean parseJsonNode(String parentKey, JsonNode jsonNode) {

        try {
            for (Iterator<Entry<String, JsonNode>> iter = jsonNode.fields(); iter.hasNext();) {
                Entry<String, JsonNode> node = iter.next();

                String key = parentKey + node.getKey();
                JsonNode value = node.getValue();

                keyStr += "`" + key + "` ";

                JsonNodeType nodeType = value.getNodeType();

                if (nodeType == JsonNodeType.OBJECT) {
                    keyStr += "JSON, ";
                    valueStr += "'" + value.toString() + "', ";
                    if (!parseJsonNode(key + ".", value)) {
                        return false;
                    }
                } else if (nodeType == JsonNodeType.ARRAY) {
                    keyStr += "JSON, ";
                    valueStr += "'" + value.toString() + "', ";
                } else if (nodeType == JsonNodeType.STRING) {
                    keyStr += ", ";
                    valueStr += "'" + value.asText() + "', ";
                } else {
                    keyStr += ", ";
                    valueStr += value.toString() + ", ";
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            return false;
        }
        return true;
    }

}
