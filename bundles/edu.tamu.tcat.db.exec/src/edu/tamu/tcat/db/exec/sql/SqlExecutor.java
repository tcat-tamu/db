package edu.tamu.tcat.db.exec.sql;

import java.sql.Connection;
import java.util.concurrent.Future;

/**
 * Runs {@link SqlExecutor.ExecutorTask}s.
 * <p>
 * An executor has lifecycle, and should be {@link #close()}d when the application determines its lifecycle is complete.
 */
public interface SqlExecutor extends AutoCloseable
{
   /**
    * Schedules a task for execution and returns a {@link Future} that the caller can use to access the result of the
    * task.
    *
    * @param task The task to submit for execution.
    * @return A {@link Future}
    */
   <X> Future<X> submit(ExecutorTask<X> task);
   
   /**
    * A task for execution by ay {@link SqlExecutor}.
    * <p>
    * Since a task may perform a complex transation involving both reads and writes, all tasks are treated similarly.
    * <p>
    * Note that the execution of a {@code Task} <strong>must not</strong> call foreign methods that may execute other tasks
    * on the same data source. Since data source access drivers are typically not declared to be thread-safe
    * and therefore all requests are handled in a single thread, any
    * other calls that need to execute against the same data source will deadlock waiting while for this task to complete.
    * 
    * @param <T> The type of result returned from the task's execution.
    */
   interface ExecutorTask<T>
   {
      /**
       * Executes this data source task.
       * <p>
       * If the task is not able to complete normally, it may perform a subset of its operation rather than throw, but
       * in that case should return an object allowing access to the sub-tasks performed and failed.
       * <p>
       * In the event of an exception, the executor for the task may attempt to roll-back any pending changes according
       * to its implementation and configuration. This may be a feature specific to certain data source implementations.
       * 
       * @param conn The data base {@link Connection} to be used as the target.
       * @return The result of the task. This may be {@code null}.
       * @throws Exception If the task is not able to complete.
       */
      T execute(Connection conn) throws Exception;
   }
}
