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
