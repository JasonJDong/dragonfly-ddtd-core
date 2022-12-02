package org.dragonfly.ddtd.curator;

import org.apache.curator.utils.PathUtils;

class GetDataOperation implements Operation
{
    private final PathChildrenCache cache;
    private final String fullPath;

    GetDataOperation(PathChildrenCache cache, String fullPath)
    {
        this.cache = cache;
        this.fullPath = PathUtils.validatePath(fullPath);
    }

    @Override
    public void invoke() throws Exception
    {
        cache.getDataAndStat(fullPath);
    }

    @Override
    public boolean equals(Object o)
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        GetDataOperation that = (GetDataOperation)o;

        //noinspection RedundantIfStatement
        if ( !fullPath.equals(that.fullPath) )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return fullPath.hashCode();
    }

    @Override
    public String toString()
    {
        return "GetDataOperation{" +
                "fullPath='" + fullPath + '\'' +
                '}';
    }
}