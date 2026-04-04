package io.pixelsdb.spark.source;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class PixelsMetadataColumns
{
    public static final String OP = "_pixels_op";
    public static final String TXN_ID = "_pixels_txn_id";
    public static final String TOTAL_ORDER = "_pixels_total_order";
    public static final String DATA_COLLECTION_ORDER = "_pixels_data_collection_order";

    private static final Set<String> COLUMN_NAMES = new HashSet<>(Arrays.asList(
            OP,
            TXN_ID,
            TOTAL_ORDER,
            DATA_COLLECTION_ORDER));

    private PixelsMetadataColumns()
    {
    }

    public static boolean isMetadataColumn(String columnName)
    {
        return COLUMN_NAMES.contains(columnName);
    }
}
