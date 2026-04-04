package io.pixelsdb.spark.source;

import org.apache.spark.sql.connector.read.streaming.Offset;

public class PixelsOffset extends Offset
{
    private final long value;

    public PixelsOffset(long value)
    {
        this.value = value;
    }

    public long getValue()
    {
        return value;
    }

    @Override
    public String json()
    {
        return Long.toString(value);
    }
}
