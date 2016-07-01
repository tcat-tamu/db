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
package edu.tamu.tcat.db.exec.sql;

import java.sql.Connection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Runs {@link SqlExecutor.ExecutorTask}s.
 */
public interface SqlExecutor
{
   /**
    * Schedules a task for execution and returns a {@link Future} that the caller can use to access the result of the
    * task.
    *
    * @param task The task to submit for execution.
    * @return A {@link Future}
    */
   <X> CompletableFuture<X> submit(ExecutorTask<X> task);

   // TODO accept multiple tasks to be run as a transaction

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

      /**
       * @since 1.2
       */
      default T execute(Connection conn, ExecutionContext context) throws Exception
      {
         return execute(conn);
      }
   }

   /**
    * @since 1.2
    */
   interface ExecutionContext
   {
      boolean isCancelled();
   }
}
