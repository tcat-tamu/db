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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import edu.tamu.tcat.db.core.DataSourceException;
import edu.tamu.tcat.db.exec.sql.SqlExecutor;
import edu.tamu.tcat.db.provider.DataSourceProvider;

/**
 * This executor implementation has lifecycle, and should be {@link #close()}d when the application determines its lifecycle is complete.
 */
public class PostgreSqlExecutor implements SqlExecutor, AutoCloseable
{
   private static final Logger debug = Logger.getLogger(PostgreSqlExecutor.class.getName());

   private ExecutorService executor;
   private DataSource dataSource;

   /**
    * Initialize this executor with a single thread for SQL task execution.
    */
   public void init(DataSourceProvider dsp) throws DataSourceException
   {
      init(dsp, Integer.valueOf(1));
   }

   /**
    * Initialize this executor with a custom number of threads for SQL task execution.
    *
    * @param numThreads If 1, 0, or negative, will be single-threaded; if {@code null}, will be unbounded,
    *        otherwise will use no more concurrent threads than the count provided (i.e. fixed pool)
    * @since 1.2
    */
   public void init(DataSourceProvider dsp, Integer numThreads) throws DataSourceException
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

      // NOTE: https://jdbc.postgresql.org/documentation/94/thread.html
      //       According to the PostgreSQL JDBC docs, the driver IS thread-safe, and will block other calls attempting
      //       to use the same Connection, so they recommend connection pooling. Apache DBCP (v1) has issues in
      //       scalability, and DBCP2 is being used successfully in some places. This executor need not be
      //       single-threaded depending on the connection pooling mechanism used.
      if (numThreads == null)
         this.executor = Executors.newCachedThreadPool();
      else if (numThreads.intValue() <= 1)
         this.executor = Executors.newSingleThreadExecutor();
      else
         this.executor = Executors.newFixedThreadPool(numThreads.intValue());
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
   public <T> CompletableFuture<T> submit(SqlExecutor.ExecutorTask<T> task)
   {
      CompletableFuture<T> result = new CompletableFuture<>();
      executor.execute(new ExecutionTaskRunner<>(task, result));
      return result;
   }

   private class ExecutionTaskRunner<T> implements Runnable
   {
      private final SqlExecutor.ExecutorTask<T> task;
      private final CompletableFuture<T> future;

      ExecutionTaskRunner(SqlExecutor.ExecutorTask<T> task, CompletableFuture<T> future)
      {
         this.task = task;
         this.future = future;
      }

      @Override
      public void run()
      {
         try (Connection conn = dataSource.getConnection())
         {
            if (future.isCancelled())
               throw new InterruptedException();

            try
            {
               conn.setAutoCommit(false);
               T result = task.execute(conn);
               conn.commit();
               future.complete(result);
            }
            catch (Exception ex)
            {
               try  {
                  conn.rollback();
               } catch (Exception e) {
                  ex.addSuppressed(e);
               }

               future.completeExceptionally(ex);
            }
         }
         catch (Exception ex) {
            // TODO add better messaging.
            future.completeExceptionally(ex);
         }
      }
   }
}
