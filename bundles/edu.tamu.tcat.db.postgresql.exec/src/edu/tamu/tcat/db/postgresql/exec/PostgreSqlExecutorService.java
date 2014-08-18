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
