# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "27540c16-ea98-4fa5-8bdd-3a8dca518958",
# META       "default_lakehouse_name": "sample_lakehouse_for_AI",
# META       "default_lakehouse_workspace_id": "2150bb83-47b4-4fde-a244-6be5b7e62908",
# META       "known_lakehouses": [
# META         {
# META           "id": "27540c16-ea98-4fa5-8bdd-3a8dca518958"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "0db0216c-7968-9652-40bd-dfcd65765b90",
# META       "known_warehouses": [
# META         {
# META           "id": "0db0216c-7968-9652-40bd-dfcd65765b90",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC # Welcome to your new notebook
# MAGIC # Type here in the cell editor to add code!
# MAGIC spark.sql("DESCRIBE TABLE Medallion").show(truncate=False)
# MAGIC spark.sql("DESCRIBE TABLE Time").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
