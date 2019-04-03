/*
 * Copyright 2019 Texas A&M Engineering Experiment Station
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
package edu.tamu.tcat.db.mssql.example;

import java.sql.Connection;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import edu.tamu.tcat.db.core.DataSourceException;
import edu.tamu.tcat.db.provider.DataSourceProvider;
import edu.tamu.tcat.osgi.config.ConfigurationProperties;

/**
 * An example {@link DataSourceProvider} using <i>HikariCP</i> and <i>MSSQL</i>.
 * This example may be copied and updated to load configuration according to the application's needs.
 * Each distinct {@link DataSource} used by the app should have a dedicated {@link DataSourceProvider}
 * with its own configuration and metadata (to distinguish among multiple) for application use.
 */
public class PostgreSqlHikariDataSourceProvider implements DataSourceProvider
{
   private static final Logger logger = Logger.getLogger(PostgreSqlHikariDataSourceProvider.class.getName());
   private static final String PROP_CONN_TIMEOUT_SEC = "edu.tamu.tcat.db.example.db.conn_timeout_s";

   private ConfigurationProperties svcProps;

   private HikariDataSource dataSource;

   public void bind(ConfigurationProperties svc)
   {
      this.svcProps = svc;
   }

   /**
    * Close all datasources<br>
    * If the provider is a service, this should be invoked on service deregistration
    */
   public void shutdown() throws DataSourceException
   {
      try
      {
         if (dataSource != null)
            dataSource.close();
      }
      catch (Exception e)
      {
         throw new DataSourceException("Error closing " + getClass().getName() + "datasource", e);
      }
   }

   void activate()
   {
      try
      {
         Objects.requireNonNull(svcProps);
         String host = svcProps.getPropertyValue("edu.tamu.tcat.db.example.db.host", String.class);
         String database = svcProps.getPropertyValue("edu.tamu.tcat.db.example.db.db", String.class);
         String user = svcProps.getPropertyValue("edu.tamu.tcat.db.example.db.user", String.class);
         String password = svcProps.getPropertyValue("edu.tamu.tcat.db.example.db.password", String.class);
         Boolean ssl = svcProps.getPropertyValue("edu.tamu.tcat.db.example.db.useSsl", Boolean.class, Boolean.FALSE);
         Integer port = svcProps.getPropertyValue("edu.tamu.tcat.db.example.db.port", Integer.class);

         Integer timeoutSec = svcProps.getPropertyValue(PROP_CONN_TIMEOUT_SEC, Integer.class, Integer.valueOf(5));

         // Create data source
         PGSimpleDataSource ds = new PGSimpleDataSource();
         ds.setServerName(host);
         if (port != null)
            ds.setPortNumber(port.intValue());
         if (ssl.booleanValue())
         {
            ds.setSsl(true);
            ds.setSslCert(null);
            // This mode encrypts traffic but does not use client-side cert config at all
            // See https://jdbc.postgresql.org/documentation/head/ssl-client.html
            ds.setSslMode("require");
         }

         ds.setDatabaseName(database);

         if (user != null && !user.isEmpty() && password != null && ! password.isEmpty())
         {
            ds.setUser(user);
            ds.setPassword(password);
         }
         else
            throw new DataSourceException("Username and password must be specified");

         //logger.info("about to test conn");

         ExecutorService exec = Executors.newSingleThreadExecutor();
         try
         {
            Future<HikariDataSource> initializer = exec.submit(() -> {
               //int maxPoolSize = Integer.parseInt(parameters.getProperty("hikari.maxpoolsize", "10")); // default = 10
               //long connTimeout = Long.parseLong(parameters.getProperty("hikari.connectiontimeout", "60000")); // default = 30s
               //long idleTimeout = Long.parseLong(parameters.getProperty("hikari.idletimeout", "600000")); // default = 10 minutes
               //int minIdle = Integer.parseInt(parameters.getProperty("hikari.minidle", String.valueOf(maxPoolSize))); // default = max pool size

               HikariConfig config = new HikariConfig();
               //logger.info("setting ds");
               config.setDataSource(ds);
               //logger.info("setting cfg");

               HikariDataSource hds = new HikariDataSource(config);// {
               //   @Override
               //   public String toString() {
               //      return super.toString() + " @ " + connectionUrl;
               //   };
               //};
               //dataSource.setIdleTimeout(idleTimeout);
               //dataSource.setMaximumPoolSize(maxPoolSize);
               //dataSource.setMinimumIdle(minIdle);
               //dataSource.setConnectionTimeout(connTimeout);
               //logger.info("connecting");

               try (Connection connection = hds.getConnection())
               {
                  logger.fine("DB connection opened successfully by " + PostgreSqlHikariDataSourceProvider.class.getName());
               }
               catch (Exception e)
               {
                  logger.log(Level.SEVERE, "Failed connecting to database in " + PostgreSqlHikariDataSourceProvider.class.getName(), e);
               }
               return hds;
            });

            try
            {
               this.dataSource = initializer.get(timeoutSec.intValue(), TimeUnit.SECONDS);
               logger.info("DB connection tested successfully by " + PostgreSqlHikariDataSourceProvider.class.getName());
            }
            catch (TimeoutException te)
            {
               logger.severe("Failed initializing DB connection (timed out) in " + getClass().getName());
               if (dataSource != null)
                  dataSource.close();
            }
            catch (Exception e)
            {
               logger.log(Level.SEVERE, "Failed initializing DB connection in " + getClass().getName(), e);
               if (dataSource != null)
                  dataSource.close();
            }
         }
         finally
         {
            exec.shutdownNow();
         }
      }
      catch (Exception e)
      {
         throw new IllegalStateException("Failed initializing MSSQL Hikari DataSource Provider", e);
      }
   }

   @Override
   public DataSource getDataSource()
   {
      if (dataSource.isClosed())
         throw new IllegalStateException("DataSource is unable to create connections");
      return dataSource;
   }
}
