/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.mpjdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Schema.TableType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Extensions;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.store.AbstractSchema;

import com.google.common.collect.ImmutableList;

import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;

class MPJdbcClient {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
       .getLogger(MPJdbcClient.class);

    private MPJdbcClientOptions clientOptions;
    private Connection conn;
    private DatabaseMetaData metadata;
    private String uri;
    private MPJdbcSchema defaultSchema;
    private MPJdbcFormatPlugin plugin;
    private String plugName;
    private BasicDataSource dataSource;
    private CalciteConnection calciteConnection;

    public MPJdbcClient(String uri, MPJdbcClientOptions clientOptions,
            MPJdbcFormatPlugin plugin) {
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
            Properties info = new Properties();
            info.setProperty("lex", "JAVA");
            Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
            calciteConnection = connection.unwrap(CalciteConnection.class);
            Class.forName(clientOptions.getDriver()).newInstance();
            dataSource = new BasicDataSource();
            dataSource.setUrl(uri);
            this.clientOptions = clientOptions;

            String user = this.clientOptions.getUser();
            String passwd = this.clientOptions.getPassword();
            this.plugin = plugin;
            this.uri = uri;

            if (user == null || user.length() == 0 || passwd.length() == 0) {
                logger.info("username, password assumed to be in the uri");
                this.conn = dataSource.getConnection();
            } else {
              dataSource.setUsername(this.clientOptions.getUser());
              dataSource.setPassword(this.clientOptions.getPassword());
              this.conn = dataSource.getConnection();
            }
            this.metadata = this.conn.getMetaData();
            this.plugName = plugin.getName();
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
           throw new DrillRuntimeException(e);
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            throw new DrillRuntimeException(e);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            throw new DrillRuntimeException(e);
        } catch (SQLException e) {
            throw new DrillRuntimeException(e);
        }
    }

    public Connection getConnection() {
        return this.conn;
    }

    public CalciteConnection getCalciteConnection() {
       return this.calciteConnection;
    }

    public BasicDataSource getBasicDataSource() {
       return this.dataSource;
    }

    public Map<String, Integer> getSchemas() {
        Map<String, Integer> lst = new HashMap<String, Integer>();
        try {
            ResultSet rs = this.metadata.getCatalogs();
            while (rs.next()) {
                Integer val = lst.get(rs.getString(1));
                if (val == null) {
                    lst.put(rs.getString(1), new Integer(1));
                }
            }

        } catch (SQLException e) {
            throw new DrillRuntimeException(e);
        }
        return lst;
    }

    public Set<String> getTables(String catalog) {
        Set<String> lst = new HashSet<String>();

        String[] typeList = { "TABLE", "VIEW" };
        try {
            ResultSet rs = this.metadata
                    .getTables(catalog,null, null, null);
            while (rs.next()) {
                if (rs.getString(1) != null) {
                  //lst.add(rs.getString(1) + "." + rs.getString("TABLE_NAME"));
                  lst.add(rs.getString("TABLE_NAME"));
                } else {
                    lst.add(rs.getString("TABLE_NAME"));
                }
            }

        } catch (SQLException e) {
            throw new DrillRuntimeException(e);
        }
        return lst;
    }

    public List<String> getDatabases() {
        List<String> lst = new ArrayList<String>();
        try {
            ResultSet rs = this.metadata.getCatalogs();
            while (rs.next()) {
                lst.add(rs.getString(0));
            }
        } catch (SQLException e) {
            throw new DrillRuntimeException(e);
        }
        return lst;
    }

    public void close() {
        // TODO Auto-generated method stub
        try {
            this.conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public MPJdbcSchema getSchema() {
        List<String> l = new ArrayList<String>();
        String currentSchema = MPJdbcCnxnManager.getClient(uri, clientOptions,
                plugin).getCurrentSchema();
        defaultSchema = new MPJdbcSchema(currentSchema);
        return defaultSchema;
    }

    public MPJdbcSchema getSchema(String name) {
        List<String> l = new ArrayList<String>();
        MPJdbcSchema schema = new MPJdbcSchema(name);
        return schema;
    }

    public class MPJdbcSchema extends AbstractSchema {

        private Map<String, Integer> sub_schemas;
        private String currentSchema;
        private Set<String> tables;

        public MPJdbcSchema(String name) {
            super(ImmutableList.<String> of(), name);
            /*currentSchema = MPJdbcCnxnManager.getClient(uri, clientOptions,
                    plugin).getCurrentSchema();
            if (currentSchema == null) {
                currentSchema = MPJdbcCnxnManager.getClient(uri, clientOptions,
                        plugin).getCurrentSchema();
            }
            */
            if(name.equals("")) {
              sub_schemas = MPJdbcCnxnManager.getClient(uri, clientOptions, plugin)
                  .getSchemas();
            }
            tables = MPJdbcCnxnManager.getClient(uri, clientOptions, plugin)
                    .getTables(name);
        }

        public MPJdbcSchema(List<String> parentSchemaPath, String name) {
            super(parentSchemaPath, name);
            currentSchema = MPJdbcCnxnManager.getClient(uri, clientOptions,
                    plugin).getCurrentSchema();
            if (currentSchema == null) {
                currentSchema = "ROOT";
            }
            sub_schemas = MPJdbcCnxnManager.getClient(uri, clientOptions, plugin)
                    .getSchemas();
            // TODO Auto-generated constructor stub
        }

        @Override
        public String getTypeName() {
            // TODO Auto-generated method stub
            return plugName;
        }

        @Override
        public AbstractSchema getSubSchema(String name) {
            if (sub_schemas == null) {
                sub_schemas = MPJdbcCnxnManager.getClient(uri, clientOptions,
                        plugin).getSchemas();
            }
            Integer a = sub_schemas.get(name);
            if (a == 1) {
                return new MPJdbcSchema(name);
            }
            return null;
        }

        @Override
        public Table getTable(String name) {
            // TODO Auto-generated method stub
          String tableName = null;
          if(name.contains(".")) {
            String[] val = name.split("\\.");
            MPJdbcSchema sub = (MPJdbcSchema) this.getSubSchema(val[0]);
            return sub.getTable(val[1]);
          }
          Iterator<String> iter = tables.iterator();
          while(iter.hasNext()) {
            tableName = iter.next();
            if(tableName.equalsIgnoreCase(name)) {
              break;
            }
            else {
              tableName = null;
            }
          }
          if(tableName == null) {
            return null;
          }
          MPJdbcScanSpec spec = new MPJdbcScanSpec(this.name, tableName,"");
          return new DynamicDrillTable(plugin, plugName, spec);
        }

        @Override
        public Set<String> getTableNames() {
            // TODO Auto-generated method stub
            Set<String> Tables = MPJdbcCnxnManager.getClient(uri, clientOptions,
                    plugin).getTables(name);
            return Tables;
        }

        @Override
        public Set<String> getSubSchemaNames() {
            // TODO Auto-generated method stub
            sub_schemas = MPJdbcCnxnManager.getClient(uri, clientOptions, plugin)
                    .getSchemas();
            return sub_schemas.keySet();
        }

        @Override
        public Collection<Function> getFunctions(String name) {
            // TODO Auto-generated method stub
            return super.getFunctions(name);
        }

        @Override
        public AbstractSchema getDefaultSchema() {
            return MPJdbcCnxnManager.getClient(uri, clientOptions, plugin)
                    .getDefaultSchema();
        }

    }

    public String getCurrentSchema() {
        // TODO Auto-generated method stub
        try {
            return this.conn.getCatalog();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public AbstractSchema getDefaultSchema() {
        // TODO Auto-generated method stub
        return defaultSchema;
    }
}
