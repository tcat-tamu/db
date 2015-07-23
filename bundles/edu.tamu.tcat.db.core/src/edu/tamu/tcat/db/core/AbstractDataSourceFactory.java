/*
 * Copyright 2014 Texas A&M Engineering Experiment Station
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

package edu.tamu.tcat.db.core;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverConnectionFactory;

/**
 * A base class for a factory providing {@link DataSource} instances. Subclasses accept configurations as
 * {@link Properties} and configure appropriate data sources.
 *
 * @see edu.tamu.tcat.db.provider.DataSourceProvider
 */
public abstract class AbstractDataSourceFactory
{
   public final static String MAX_ACTIVE_CONNECTIONS = "Max Active Connections";
   public final static String MAX_IDLE_CONNECTIONS   = "Max Idle Connections";

    //FIXME: a Properties is an unsafe map key because it is mutable.
    protected Map<Properties, BasicDataSource> dataSources = new HashMap<>();

    /**
     * Get the driver for the concrete database type
     * */
    protected abstract Driver getDriver() throws DataSourceException;

    /**
     * Get the connection properties for opening the connection<br>
     * This should include user, and password.  It should not include database or host name
     * */
    protected abstract Properties getConnectionProperties(Properties parameters);

    /**
     * Get the connection url specifying the database driver, host/file, and database name
     * */
    protected abstract String getConnectionUrl(Properties parameters) throws DataSourceException;

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
        if(parameters.containsKey(MAX_ACTIVE_CONNECTIONS))
            return Integer.parseInt(parameters.getProperty(MAX_ACTIVE_CONNECTIONS));
        return 5;
    }

    protected int getMaxIdleConnections(Properties parameters)
    {
        if(parameters.containsKey(MAX_IDLE_CONNECTIONS))
            return Integer.parseInt(parameters.getProperty(MAX_IDLE_CONNECTIONS));
        return 5;
    }

    public int getNumActive(DataSource ds) throws DataSourceException
    {
        if(ds instanceof BasicDataSource)
            return ((BasicDataSource )ds).getNumActive();
        throw new DataSourceException("getNumActive not supported");
    }
}
