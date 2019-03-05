/*
 * Copyright 2014-2019 Texas A&M Engineering Experiment Station
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.tamu.tcat.db.postgresql;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverConnectionFactory;

import edu.tamu.tcat.db.core.DataSourceException;

/**
 *
 * @deprecated A simpler and more custom per-application solution should be used. Since 1.5, the base
 *             class has been removed (which should have been a 2.0 breaking change), but this class
 *             is to be removed in 2.0 in favor of something like the example provided.
 */
@Deprecated
public class PostgreSqlDataSourceFactory
{
   protected Driver driver = null;

   /** @since 1.5 */
   public final static String MAX_ACTIVE_CONNECTIONS = "Max Active Connections";
   /** @since 1.5 */
   public final static String MAX_IDLE_CONNECTIONS   = "Max Idle Connections";

   //FIXME: a Properties is an unsafe map key because it is mutable.
   protected Map<Properties, BasicDataSource> dataSources = new HashMap<>();

   /**
    * Create a new {@link BasicDataSource} from the specified {@link Properties}
    * @since 1.5
    */
   protected synchronized BasicDataSource createDataSource(final Properties parameters) throws DataSourceException
   {
      BasicDataSource dataSource;
      final Driver driver = getDriver();
      final String connectionUrl = getConnectionUrl(parameters);
      final Properties connectionProps = getConnectionProperties(parameters);

      dataSource = new BasicDataSource()
      {
         @Override
         protected ConnectionFactory createConnectionFactory() throws SQLException
         {
            //The loading of the driver via class-loader does not work properly in OSGI.

            String dataSourceUrl = getUrl();
            if (!driver.acceptsURL(dataSourceUrl))
               throw new IllegalStateException("Invalid database URL provided to driver: " + dataSourceUrl);

            if (getValidationQuery() == null)
            {
               setTestOnBorrow(false);
               setTestOnReturn(false);
               setTestWhileIdle(false);
            }

            ConnectionFactory driverConnectionFactory = new DriverConnectionFactory(driver, connectionUrl, connectionProps);
            return driverConnectionFactory;
         }

         @Override
         public String toString()
         {
            return "DataSource["+getUrl()+"]";
         }
      };
      //         dataSource.setDriverClassLoader(Driver.class.getClassLoader());
      // should be included in the connection properties and not needed
      //        dataSource.setUsername(key.getUsername());
      //        dataSource.setPassword(key.getPassword());
      dataSource.setDriverClassName(driver.getClass().getName());
      dataSource.setUrl(connectionUrl);
      dataSource.setMaxActive(getMaxActiveConnections(parameters));
      dataSource.setMaxIdle(getMaxIdleConnections(parameters));
      dataSource.setMinIdle(0);
      dataSource.setMinEvictableIdleTimeMillis(10000);
      dataSource.setTimeBetweenEvictionRunsMillis(1000);
      dataSource.setLogAbandoned(true);
      dataSource.setRemoveAbandoned(true);//seconds
      dataSource.setRemoveAbandonedTimeout(60);
      return dataSource;
   }

   // expose as javax.sql.DataSource - if exposed as org.apache.commons.dbcp.BasicDataSource, clients
   // of this class must import that package to link to any factory subclass
   /** @since 1.5 */
   public DataSource getDataSource(Properties parameters) throws DataSourceException
   {
      BasicDataSource dataSource = dataSources.get(parameters);
      if (dataSource == null)
      {
         Properties nonMutating = new Properties();
         nonMutating.putAll(parameters);
         dataSource = createDataSource(nonMutating);
         dataSources.put(nonMutating, dataSource);
      }
      return dataSource;
   }

   /**
    * Close all datasources<br>
    * If the provider is a service, this should be invoked on service deregistration
    * @since 1.5
    */
   public void shutdown() throws DataSourceException
   {
      DataSourceException exception = new DataSourceException("Error closing " + getClass().getName() + "datasources");
      for (BasicDataSource ds : dataSources.values())
      {
         try
         {
            ds.close();
         }
         catch (Exception e)
         {
            exception.addSuppressed(exception);
         }
      }
      if (exception.getSuppressed().length != 0)
         throw exception;
   }

   /** @since 1.5 */
   protected int getMaxActiveConnections(Properties parameters)
   {
      if(parameters.containsKey(MAX_ACTIVE_CONNECTIONS))
         return Integer.parseInt(parameters.getProperty(MAX_ACTIVE_CONNECTIONS));
      return 5;
   }

   /** @since 1.5 */
   protected int getMaxIdleConnections(Properties parameters)
   {
      if(parameters.containsKey(MAX_IDLE_CONNECTIONS))
         return Integer.parseInt(parameters.getProperty(MAX_IDLE_CONNECTIONS));
      return 5;
   }

   /** @since 1.5 */
   public int getNumActive(DataSource ds) throws DataSourceException
   {
      if(ds instanceof BasicDataSource)
         return ((BasicDataSource )ds).getNumActive();
      throw new DataSourceException("getNumActive not supported");
   }

   public PostgreSqlPropertiesBuilder getPropertiesBuilder()
   {
      return new PostgreSqlPropertiesBuilder();
   }

   /**
    * Get the connection url specifying the database driver, host/file, and database name
    */
   protected String getConnectionUrl(Properties parameters) throws DataSourceException
   {
      return getConnectionUrl(parameters.getProperty(PostgreSqlPropertiesBuilder.HOST),
            PostgreSqlPropertiesBuilder.getPort(parameters),
            parameters.getProperty(PostgreSqlPropertiesBuilder.DATABASE));
   }

   /**
    * Get the connection properties for opening the connection<br>
    * This should include user, and password.  It should not include database or host name
    */
   protected Properties getConnectionProperties(Properties parameters)
   {
      Properties prop = new Properties();
      prop.putAll(parameters);
      // Remove properties used to pass into the PostgreSqlPropertiesBuilder that do not belong in the set sent to PostgreSQL
      prop.remove(PostgreSqlPropertiesBuilder.HOST);
      prop.remove(PostgreSqlPropertiesBuilder.DATABASE);
      prop.remove(PostgreSqlPropertiesBuilder.PORT);

      return prop;
   }

   protected String getConnectionUrl(String server, int port, String database) throws DataSourceException
   {
      StringBuilder sb = new StringBuilder();
      sb.append("jdbc:postgresql://");

      if (server == null)
      {
         throw new DataSourceException("Could not construct database URL. No host specified");
      }
      sb.append(server);

      if (port >= 0)
      {
         sb.append(":").append(port);
      }

      if (database == null)
      {
         throw new DataSourceException("Could not construct database URL. No database specified");
      }

      sb.append("/").append(database);
      return sb.toString();
   }

   /**
    * Get the driver for the concrete database type
    */
   protected Driver getDriver() throws DataSourceException
   {
      if (driver == null)
         driver = new org.postgresql.Driver();
      return driver;
   }
}
