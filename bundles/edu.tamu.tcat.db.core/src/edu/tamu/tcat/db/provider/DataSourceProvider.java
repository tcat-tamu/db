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

package edu.tamu.tcat.db.provider;

import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * A simple provider of a single, pre-configured {@link DataSource}. This interface should be
 * preferred in applications that only use a single main data source, in which case this API can be
 * provided by a service. The service implementation could abstract the configuration from the
 * data source access by reading configuration during some initialization step.
 * 
 * @see edu.tamu.tcat.db.core.AbstractDataSourceFactory
 */
public interface DataSourceProvider
{
   /**
    * @return The data source configured for this provider. Does not return <code>null</code>
    * @throws SQLException If the data source could not be initialized.
    */
   DataSource getDataSource() throws SQLException;
}
