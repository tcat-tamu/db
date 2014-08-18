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
   public static final Logger DB_LOGGER = Logger.getLogger("edu.tamu.tcat.oss.db.hsqldb");

   private ExecutorService executor;
   private DataSource dataSource;

   public PostgreSqlExecutor()
   {
   }

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
            DB_LOGGER.log(Level.SEVERE, "SqlExecutor failed to complete all tasks.");
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