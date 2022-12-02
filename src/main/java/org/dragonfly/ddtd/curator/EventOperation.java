package org.dragonfly.ddtd.curator;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

class EventOperation implements Operation
{
    private final PathChildrenCache cache;
    private final PathChildrenCacheEvent event;

    EventOperation(PathChildrenCache cache, PathChildrenCacheEvent event)
    {
        this.cache = cache;
        this.event = event;
    }

    @Override
    public void invoke()
    {
        cache.callListeners(event);
    }

    @Override
    public String toString()
    {
        return "EventOperation{" +
                "event=" + event +
                '}';
    }
}
