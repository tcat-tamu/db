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
package edu.tamu.tcat.db.postgresql.exec;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import edu.tamu.tcat.db.core.DataSourceException;
import edu.tamu.tcat.db.exec.sql.SqlExecutor;
import edu.tamu.tcat.db.provider.DataSourceProvider;

public class PostgreSqlExecutor implements SqlExecutor
{
   private static final Logger debug = Logger.getLogger(PostgreSqlExecutor.class.getName());

   private ExecutorService executor;
   private DataSource dataSource;

   public void init(DataSourceProvider dsp) throws DataSourceException
   {
      try
      {
         dataSource = dsp.getDataSource();
      }
      catch (SQLException e)
      {
         throw new DataSourceException("Failed to access data source", e);
      }

      //TODO: should a watchdog thread be added to kill tasks that take too long?
      this.executor = Executors.newSingleThreadExecutor();
   }

   @Override
   public void close()
   {
      if (executor != null)
      {
         boolean terminated = false;
         try
         {
            executor.shutdown();
            terminated = executor.awaitTermination(30, TimeUnit.SECONDS);
         }
         catch (InterruptedException e)
         {
            terminated = false;
         }

         if (!terminated)
         {
            debug.log(Level.SEVERE, "SqlExecutor failed to complete all tasks.");
            executor.shutdownNow();
         }
      }
   }

   @Override
   public <T> Future<T> submit(SqlExecutor.ExecutorTask<T> task)
   {
      // TODO to allow cancellation, timeouts, etc, should grab returned future.
      ExecutionTaskRunner<T> runner = new ExecutionTaskRunner<T>(dataSource, task);
      return executor.submit(runner);
   }

   private static class ExecutionTaskRunner<T> implements Callable<T>
   {
      private final SqlExecutor.ExecutorTask<T> task;
      private final DataSource ds;

      ExecutionTaskRunner(DataSource ds, SqlExecutor.ExecutorTask<T> task)
      {
         this.ds = ds;
         this.task = task;
      }

      @Override
      public T call() throws Exception
      {
         try (Connection conn = ds.getConnection())
         {
            try
            {
               return task.execute(conn);
            }
            catch (Exception ex)
            {
               try
               {
                  conn.rollback();
               }
               catch (Exception e)
               {
                  ex.addSuppressed(e);
               }

               throw ex;
            }
         }
      }
   }
}
