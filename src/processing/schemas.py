from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

def get_binance_schema():
    """
    Defines the exact structure of the incoming JSON payload from Binance.
    """
    return StructType([
        StructField("e", StringType(), True),   # Event type
        StructField("E", LongType(), True),     # Event time (Unix epoch in ms)
        StructField("s", StringType(), True),   # Symbol
        StructField("p", StringType(), True),   # Price (comes as string from Binance)
        StructField("q", StringType(), True),   # Quantity (comes as string)
        StructField("T", LongType(), True)      # Trade time
    ])