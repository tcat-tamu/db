/*
 * Copyright 2014 Texas A&M Engineering Experiment Station
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverConnectionFactory;

import edu.tamu.tcat.db.core.AbstractDataSourceFactory;
import edu.tamu.tcat.db.core.DataSourceException;

public class PostgreSqlDbcp2DataSourceFactory
{
   // The Properties key is ensured immutable to make it a safe key
   private Map<Properties, BasicDataSource> dataSources = new HashMap<>();
   private Driver cachedDriver = null;

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
      if (cachedDriver == null)
         cachedDriver = new org.postgresql.Driver();
      return cachedDriver;
   }

   /**
    * Create a new {@link BasicDataSource} from the specified {@link DSProperties}
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
      //dataSource.setDriverClassLoader(Driver.class.getClassLoader());
      // user/pwd should be included in the connection properties and not needed
      //dataSource.setUsername(key.getUsername());
      //dataSource.setPassword(key.getPassword());
      dataSource.setDriverClassName(driver.getClass().getName());
      dataSource.setUrl(connectionUrl);
      dataSource.setMaxTotal(getMaxActiveConnections(parameters));
      dataSource.setMaxIdle(getMaxIdleConnections(parameters));
      dataSource.setMinIdle(0);
      dataSource.setMinEvictableIdleTimeMillis(10000);
      dataSource.setTimeBetweenEvictionRunsMillis(1000);
      dataSource.setLogAbandoned(true);
      dataSource.setRemoveAbandonedOnBorrow(true);
      dataSource.setRemoveAbandonedOnMaintenance(true);
      dataSource.setRemoveAbandonedTimeout(60); // seconds
      return dataSource;
   }

   // expose as javax.sql.DataSource - if exposed as org.apache.commons.dbcp.BasicDataSource, clients
   // of this class must import that package to link to any factory subclass
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
    * */
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

   protected int getMaxActiveConnections(Properties parameters)
   {
      if(parameters.containsKey(AbstractDataSourceFactory.MAX_ACTIVE_CONNECTIONS))
         return Integer.parseInt(parameters.getProperty(AbstractDataSourceFactory.MAX_ACTIVE_CONNECTIONS));
      return 5;
   }

   protected int getMaxIdleConnections(Properties parameters)
   {
      if(parameters.containsKey(AbstractDataSourceFactory.MAX_IDLE_CONNECTIONS))
         return Integer.parseInt(parameters.getProperty(AbstractDataSourceFactory.MAX_IDLE_CONNECTIONS));
      return 5;
   }

   public int getNumActive(DataSource ds) throws DataSourceException
   {
      if(ds instanceof BasicDataSource)
         return ((BasicDataSource )ds).getNumActive();
      throw new DataSourceException("getNumActive not supported");
   }
}
