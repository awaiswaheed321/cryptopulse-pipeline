def write_to_cassandra(batch_df, batch_id):
    """
    Spark foreachBatch sink function to write static micro-batches into Cassandra.
    # batch_df is a static DataFrame containing just the updated rows for this micro-batch
    """
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "cryptopulse") \
        .option("table", "real_time_aggregates") \
        .mode("append") \
        .save()