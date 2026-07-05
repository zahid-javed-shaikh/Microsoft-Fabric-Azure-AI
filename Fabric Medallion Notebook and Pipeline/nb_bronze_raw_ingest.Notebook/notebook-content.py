# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6aca0269-5052-49af-b2c5-635f48ffb667",
# META       "default_lakehouse_name": "LH_Logibuddy",
# META       "default_lakehouse_workspace_id": "2150bb83-47b4-4fde-a244-6be5b7e62908",
# META       "known_lakehouses": [
# META         {
# META           "id": "6aca0269-5052-49af-b2c5-635f48ffb667"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "52904025-4541-a28c-4803-393657cf19fb",
# META       "known_warehouses": [
# META         {
# META           "id": "52904025-4541-a28c-4803-393657cf19fb",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ── Connection parameters ──────────────────────────────────────
# ── Connection parameters ──────────────────────────────────────
jdbc_url    = "jdbc:mysql://auth-db1734.hstgr.io:3306/u538195328_b2c_zahid"
db_user     = "u538195328_b2c_user"
db_password = "a#DE!b0oj7L2"
driver      = "com.mysql.cj.jdbc.Driver"

# ── Step 1: Discover tables using dbtable + subquery syntax ─────
tables_df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", driver) \
    .option("dbtable", "(SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'u538195328_b2c_zahid' AND TABLE_TYPE = 'BASE TABLE') AS t") \
    .load()

table_names = [row[0] for row in tables_df.collect()]
print("Tables found:", table_names)

# ── Step 2: Load each table into Lakehouse bronze layer ─────────
for table in table_names:
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("user", db_user) \
            .option("password", db_password) \
            .option("driver", driver) \
            .load()

        df.write.format("delta").mode("overwrite").saveAsTable(f"bronze_{table}")
        print(f"✅ bronze_{table} — {df.count()} rows")

    except Exception as e:
        print(f"❌ Skipped {table}: {str(e)[:100]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
