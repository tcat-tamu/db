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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A set of utilities to help build database entities in PostgreSQL, such as databases and schemas.
 * @since 1.4
 */
public class PostgreSqlEntityHelper
{
   private static final Logger debug = Logger.getLogger(PostgreSqlEntityHelper.class.getName());

   /**
    * @param connDefaultDb A connection to the db server on the database "postgres"
    * @param targetDatabase The name of the database to create
    */
   public static boolean createDatabase(Connection connDefaultDb, String targetDatabase)
   {
      String sqlCheck = "SELECT EXISTS (SELECT * FROM pg_catalog.pg_database WHERE datname = ?)";
      try (PreparedStatement existsCheck = connDefaultDb.prepareStatement(sqlCheck))
      {
         existsCheck.setString(1, targetDatabase);
         try (ResultSet results = existsCheck.executeQuery())
         {
            if (!results.next())
            {
               debug.log(Level.WARNING, "Failed testing for database existence ["+sqlCheck+"]");
               return false;
            }

            boolean exists = results.getBoolean(1);
            if (exists)
               return false;
         }
      }
      catch (Exception e)
      {
         debug.log(Level.SEVERE, "Failed checking for existence of database ["+targetDatabase+"] with sql ["+sqlCheck+"]", e);
         // don't try to create if exist check failed
         return false;
      }

      debug.log(Level.INFO, "Database [" + targetDatabase + "] not found, creating it now");
      String sqlCreate = "CREATE DATABASE \"" + targetDatabase + "\"";
      try (PreparedStatement create = connDefaultDb.prepareStatement(sqlCreate))
      {
         create.executeUpdate();
         return true;
      }
      catch (Exception e)
      {
         debug.log(Level.SEVERE, "Failed creating database ["+targetDatabase+"] with sql ["+sqlCreate+"]", e);
         return false;
      }
   }

   /**
    * Create the "postgis" extension on the database for the given connection. The extension module
    * must already be installed on the database server, and this will ensure it is available in the database.
    *
    * @param conn
    */
   public static boolean createExtensionPostGis(Connection conn)
   {
      String sqlCheck = "SELECT EXISTS(select * from pg_extension where extname = 'postgis')";
      try (PreparedStatement existsCheck = conn.prepareStatement(sqlCheck))
      {
         try (ResultSet results = existsCheck.executeQuery())
         {
            if (!results.next())
            {
               debug.log(Level.WARNING, "Failed testing for extension existence ["+sqlCheck+"]");
               return false;
            }

            boolean exists = results.getBoolean(1);
            // schema already exists, exit early
            if (exists)
               return false;
         }
      }
      catch (Exception e)
      {
         debug.log(Level.SEVERE, "Failed checking for existence of extension [postgis] with sql ["+sqlCheck+"]", e);
         // don't try to create if exist check failed
         return false;
      }

      debug.log(Level.INFO, "Installing PostGIS extensions");
      try (PreparedStatement install = conn.prepareStatement("CREATE EXTENSION postgis"))
      {
         install.executeUpdate();
      }
      catch (Exception e)
      {
         debug.log(Level.WARNING, "Cannot create PostGIS extension, the package may need to be installed on the db server", e);
      }
      return true;
   }

   //NOTE: if these SQL statements fail, things are likely still okay; if the db is in trouble, using the schema later will fail
   public static boolean createSchema(Connection conn, String targetSchemaName)
   {
      String sqlCheck = "SELECT EXISTS(SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?)";
      try (PreparedStatement existsCheck = conn.prepareStatement(sqlCheck))
      {
         existsCheck.setString(1, targetSchemaName);
         try (ResultSet results = existsCheck.executeQuery())
         {
            if (!results.next())
            {
               debug.log(Level.WARNING, "Failed testing for schema existence ["+sqlCheck+"]");
               return false;
            }

            boolean exists = results.getBoolean(1);
            // schema already exists, exit early
            if (exists)
               return false;
         }
      }
      catch (Exception e)
      {
         debug.log(Level.SEVERE, "Failed checking for existence of schema ["+targetSchemaName+"] with sql ["+sqlCheck+"]", e);
         // don't try to create if exist check failed
         return false;
      }

      debug.log(Level.INFO, "Schema [" + targetSchemaName + "] not found, creating it now");
      String sqlCreate = "CREATE SCHEMA \"" + targetSchemaName + "\"";
      try (PreparedStatement create = conn.prepareStatement(sqlCreate))
      {
         debug.log(Level.INFO, "Schema [" + targetSchemaName + "] not found, creating it now");
         create.executeUpdate();
         return true;
      }
      catch (Exception e)
      {
         debug.log(Level.SEVERE, "Failed creating schema ["+targetSchemaName+"] with sql ["+sqlCreate+"]", e);
         return false;
      }
   }
}
