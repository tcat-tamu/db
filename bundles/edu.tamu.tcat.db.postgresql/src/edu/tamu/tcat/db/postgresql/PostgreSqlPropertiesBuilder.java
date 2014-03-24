/*******************************************************************************
 * Copyright © 2007-14, All Rights Reserved.
 * Texas Center for Applied Technology
 * Texas A&M Engineering Experiment Station
 * The Texas A&M University System
 * College Station, Texas, USA 77843
 *
 * Use is granted only to authorized licensee.
 * Proprietary information, not for redistribution.
 ******************************************************************************/

package edu.tamu.tcat.db.postgresql;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import edu.tamu.tcat.db.core.AbstractDataSourceFactory;
import edu.tamu.tcat.db.core.AbstractDatasourcePropertiesBuilder;

public class PostgreSqlPropertiesBuilder extends AbstractDatasourcePropertiesBuilder
{
    public static final String USER     = "user";
    public static final String DOMAIN   = "domain";
    public static final String PASSWORD = "password";

    // do these need to be removed from create in ds
    public static final String HOST     = "Database Server";
    public static final String DATABASE = "Database Name";
    public static final String PORT     = "Port";

    // package private to prevent non factory construction
    PostgreSqlPropertiesBuilder(){}

    static public int getPort(Properties props)
    {
        if (props.containsKey(PORT))
            return Integer.parseInt(props.getProperty(PORT));
        // should probably throw instead
        return -1;
    }

    public PostgreSqlPropertiesBuilder setUrl(String url) throws URISyntaxException
    {
        URI uri = new URI(url);
        if (uri.getScheme().equals("jdbc"))
        {
            uri = new URI(uri.getSchemeSpecificPart());
        }
        String path = uri.getPath();
        if (path.startsWith("/"))
        {
            // strip leading slash
            path = path.substring(1);
        }
        setDatabase(path);
        setHost(uri.getHost());
        setPort(uri.getPort());
        return this;
    }


    public PostgreSqlPropertiesBuilder setUser(String username)
    {
        String user = username;
        if (user != null)
        {
            int index = user.indexOf('\\');
            if (index > 0)
            {
                properties.setProperty(DOMAIN, user.substring(0, index));
                user = user.substring(index + 1);
            }
            properties.setProperty(USER, user);
        }else
            properties.remove(USER);
        return this;
    }

    public PostgreSqlPropertiesBuilder setPassword(String password)
    {
        if(password == null)
            properties.remove(PASSWORD);
        else
            properties.setProperty(PASSWORD, password);
        return this;
    }

    public PostgreSqlPropertiesBuilder setPort(int port)
    {
        if (port > -1)
            properties.setProperty(PORT, String.valueOf(port));
        else
            properties.remove(PORT);
        return this;
    }

    public PostgreSqlPropertiesBuilder setDatabase(String database)
    {
        if(database == null)
            properties.remove(DATABASE);
        else
            properties.setProperty(DATABASE, database);
        return this;
    }

    public PostgreSqlPropertiesBuilder setHost(String server)
    {
        if(server == null)
            properties.remove(HOST);
        else
            properties.setProperty(HOST, server);
        return this;
    }

    public PostgreSqlPropertiesBuilder setDomain(String domain)
    {
        if(domain == null)
            properties.remove(DOMAIN);
        else
            properties.setProperty(DOMAIN, domain);
        return this;
    }

    public PostgreSqlPropertiesBuilder create(String url, String username, String password) throws URISyntaxException
    {
        PostgreSqlPropertiesBuilder builder = new PostgreSqlPropertiesBuilder();
        return builder.setUrl(url).setUser(username).setPassword(password);
    }

    public PostgreSqlPropertiesBuilder create(String server, String database, String username, String password)
    {
        return create(server, -1, database, username, password);
    }

    public PostgreSqlPropertiesBuilder create(String server, int port, String database, String username, String password)
    {
        PostgreSqlPropertiesBuilder builder = new PostgreSqlPropertiesBuilder();
        return builder.setHost(server).setPort(port).setDatabase(database).setUser(username).setPassword(password);
    }

    public PostgreSqlPropertiesBuilder setMaxActiveConnections(int max)
    {
        if (max < 1)
            max = 1;
        properties.setProperty(AbstractDataSourceFactory.MAX_ACTIVE_CONNECTIONS, String.valueOf(max));
        return this;
    }

}
