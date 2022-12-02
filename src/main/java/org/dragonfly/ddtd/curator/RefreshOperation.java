package org.dragonfly.ddtd.curator;

class RefreshOperation implements Operation
{
    private final PathChildrenCache cache;
    private final PathChildrenCache.RefreshMode mode;

    RefreshOperation(PathChildrenCache cache, PathChildrenCache.RefreshMode mode)
    {
        this.cache = cache;
        this.mode = mode;
    }

    @Override
    public void invoke() throws Exception
    {
        cache.refresh(mode);
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

        RefreshOperation that = (RefreshOperation)o;

        //noinspection RedundantIfStatement
        if ( mode != that.mode )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return mode.hashCode();
    }

    @Override
    public String toString()
    {
        return "RefreshOperation(" + mode + "){}";
    }
}
