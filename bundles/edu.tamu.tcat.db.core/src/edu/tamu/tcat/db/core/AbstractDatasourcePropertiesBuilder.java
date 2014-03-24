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

package edu.tamu.tcat.db.core;

import java.util.Properties;

public class AbstractDatasourcePropertiesBuilder
{
    protected Properties properties = new Properties();

    public Properties getProperties()
    {
        return properties;
    }
}
