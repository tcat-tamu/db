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

package edu.tamu.tcat.db.postgresql;

import java.sql.Driver;
import java.util.Properties;

import edu.tamu.tcat.db.core.AbstractDataSourceFactory;
import edu.tamu.tcat.db.core.DataSourceException;

public class PostgreSqlDataSourceFactory extends AbstractDataSourceFactory
{
    protected Driver driver = null;

    public PostgreSqlPropertiesBuilder getPropertiesBuilder()
    {
        return new PostgreSqlPropertiesBuilder();
    }

    @Override
    protected String getConnectionUrl(Properties parameters) throws DataSourceException
    {
        return getConnectionUrl(parameters.getProperty(PostgreSqlPropertiesBuilder.HOST),
                                PostgreSqlPropertiesBuilder.getPort(parameters),
                                parameters.getProperty(PostgreSqlPropertiesBuilder.DATABASE));
    }

    @Override
    protected Properties getConnectionProperties(Properties parameters)
    {
        Properties prop = new Properties(parameters);
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

    @Override
    protected Driver getDriver() throws DataSourceException
    {
        if (driver == null)
            driver = new org.postgresql.Driver();
        return driver;
    }
}
