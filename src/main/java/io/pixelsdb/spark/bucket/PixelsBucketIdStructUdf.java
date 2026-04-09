package io.pixelsdb.spark.bucket;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF3;

public class PixelsBucketIdStructUdf implements UDF3<Row, String, Integer, Integer>
{
    @Override
    public Integer call(Row pkRow, String specString, Integer configuredBucketCount)
    {
        if (configuredBucketCount == null)
        {
            return null;
        }
        return PixelsBucketId.computeBucketId(pkRow, specString, configuredBucketCount);
    }
}
