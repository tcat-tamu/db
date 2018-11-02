package edu.tamu.tcat.db.mssql;

import java.sql.Driver;
import java.util.Properties;

import edu.tamu.tcat.db.core.AbstractDataSourceFactory;
import edu.tamu.tcat.db.core.DataSourceException;

public class MsSqlDataSourceFactory  extends AbstractDataSourceFactory
{
    protected Driver driver = null;

    public MsSqlPropertiesBuilder getPropertiesBuilder()
    {
        return new MsSqlPropertiesBuilder();
    }

    @Override
    protected String getConnectionUrl(Properties parameters) throws DataSourceException
    {
        return getConnectionUrl(parameters.getProperty(MsSqlPropertiesBuilder.HOST),
                                MsSqlPropertiesBuilder.getPort(parameters),
                                parameters.getProperty(MsSqlPropertiesBuilder.DATABASE));
    }

    @Override
    protected Properties getConnectionProperties(Properties parameters)
    {
        Properties prop = new Properties();
        prop.putAll(parameters);
        // Remove properties used to pass into the MsSqlPropertiesBuilder that do not belong in the set sent to MsSQL
        prop.remove(MsSqlPropertiesBuilder.HOST);
       // prop.remove(MsSqlPropertiesBuilder.DATABASE);
        prop.remove(MsSqlPropertiesBuilder.PORT);

        return prop;
    }

    protected String getConnectionUrl(String server, int port, String database) throws DataSourceException
    {
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:sqlserver://");

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

//        sb.append(";database=").append(database);
        return sb.toString();
    }

    @Override
    protected Driver getDriver() throws DataSourceException
    {
        if (driver == null)
            driver = new com.microsoft.sqlserver.jdbc.SQLServerDriver();
        return driver;
    }
}

