package edu.tamu.tcat.db.mssql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MsSqlEntityHelper {
   private static final Logger debug = Logger.getLogger(MsSqlEntityHelper.class.getName());

   /**
    * @param connDefaultDb A connection to the db server on the database "postgres"
    * @param targetDatabase The name of the database to create
    */
   public static boolean createDatabase(Connection connDefaultDb, String targetDatabase)
   {
      // optional "SELECT EXISTS (SELECT name FROM master.sys.databases WHERE name = ?)";
      String sqlCheck = "SELECT EXISTS (db_id(?))";
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

// no need for postgis equivalent since geometry (flat coord) and geography ( round coord) built in as base types

   //NOTE: if these SQL statements fail, things are likely still okay; if the db is in trouble, using the schema later will fail
   public static boolean createSchema(Connection conn, String targetSchemaName)
   {
      String sqlCheck = "SELECT count(schema_name) FROM INFORMATION_SCHEMA.SCHEMATA where schema_name = ?";
      try (PreparedStatement existsCheck = conn.prepareStatement(sqlCheck))
      {
         existsCheck.setString(1, targetSchemaName);
         try (ResultSet results = existsCheck.executeQuery())
         {
            if (!results.next())
            {
               //debug.log(Level.WARNING, "Failed testing for schema existence ["+sqlCheck+"]");
               //return false;
            }

            int count = results.getInt(1);
            // schema already exists, exit early
            if (count > 0)
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

