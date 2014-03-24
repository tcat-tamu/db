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

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

// TODO replace with 1.7 multi-exception
public class DataSourceCompositeException extends DataSourceException
{
    protected List<DataSourceException> nested = new ArrayList<DataSourceException>(1);
    int indent = 1;

    public DataSourceCompositeException()
    {
    }

    public DataSourceCompositeException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public DataSourceCompositeException(String message)
    {
        super(message);
    }

    public DataSourceCompositeException(Throwable cause)
    {
        super(cause);
    }

    public void addException(DataSourceException ice)
    {
        nested.add(ice);
        if(ice instanceof DataSourceCompositeException)
            ((DataSourceCompositeException)ice).increaseIndent();
    }

    private void increaseIndent()
    {
        indent++;
        for(DataSourceException ice:nested)
            if(ice instanceof DataSourceCompositeException)
                ((DataSourceCompositeException)ice).increaseIndent();
    }

    // public void addAllExceptions(CompositeInvalidConfigurationException cice)
    // {
    // addException(new InvalidConfigurationException(cice));
    // for (InvalidConfigurationException ice : cice.nested)
    // addException(ice);
    // }

    public Iterable<DataSourceException> getNested()
    {
        return new ArrayList<DataSourceException>(nested);
    }

    public boolean hasNested()
    {
        return !nested.isEmpty();
    }

    @Override
    public void printStackTrace(PrintStream s)
    {
        super.printStackTrace(s);
        s.append("Caused by:\n");
        for (Throwable t : getNested())
            t.printStackTrace(s);
    }

    @Override
    public void printStackTrace(PrintWriter s)
    {
        super.printStackTrace(s);
        s.append("Caused by:\n");
        for (Throwable t : getNested())
            t.printStackTrace(s);
    }

    @Override
    public String getMessage()
    {
        StringBuffer buff = new StringBuffer(super.getMessage() + ":");
        for (DataSourceException ice : nested)
        {
            buff.append('\n');
            for (int i = 0; i < indent; i++ )
                buff.append("  ");
            buff.append(ice.getMessage());
        }
        return buff.toString();
    }

    public String getDetailMessage()
    {
        return super.getMessage();
    }

    public boolean hasNested(Class<?> seeking)
    {
        boolean retVal = false;
        for (DataSourceException ice : nested)
        {
            if(ice instanceof DataSourceCompositeException)
                retVal |= ((DataSourceCompositeException) ice).hasNested(seeking);
            else
                retVal |= seeking.isAssignableFrom(ice.getClass());
            if(retVal)
                return true;
        }
        return retVal;
    }

    public boolean onlyHasNested(Class<?> seeking)
    {
        boolean retVal = true;
        for (DataSourceException ice : nested)
        {
            if(ice instanceof DataSourceCompositeException)
                retVal &= ((DataSourceCompositeException) ice).hasNested(seeking);
            else
                retVal &= seeking.isAssignableFrom(ice.getClass());
            if(!retVal)
                return retVal;
        }
        return retVal;
    }
}
