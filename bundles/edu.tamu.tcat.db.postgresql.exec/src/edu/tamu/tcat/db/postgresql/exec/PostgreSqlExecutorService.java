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

import edu.tamu.tcat.db.provider.DataSourceProvider;

/**
 * A {@link PostgreSqlExecutor} more suitable to be used as an OSGI declarative service implementation.
 */
public class PostgreSqlExecutorService extends PostgreSqlExecutor
{
   private DataSourceProvider bindProvider;

   public void bind(DataSourceProvider dsp)
   {
      this.bindProvider = dsp;
   }
   
   public void activate()
   {
      try
      {
         init(bindProvider);
      }
      catch (Exception e)
      {
         throw new IllegalStateException("Failed initialization", e);
      }
   }
   
   public void dispose()
   {
      close();
   }

}
