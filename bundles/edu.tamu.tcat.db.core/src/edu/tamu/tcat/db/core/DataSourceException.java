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

public class DataSourceException extends Exception
{
    public DataSourceException()
    {
    }

    public DataSourceException(String message)
    {
        super(message);
    }

    public DataSourceException(Throwable cause)
    {
        super(cause);
    }

    public DataSourceException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
