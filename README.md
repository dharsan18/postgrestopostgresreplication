# Postgres to Postgres Replication with CDC Native Logical Replication and Monitoring

This repository contains instructions and scripts to set up PostgreSQL to PostgreSQL replication using CDC (Change Data Capture) native logical replication. Additionally, it includes tools for monitoring the replication process.

## Prerequisites

Before setting up replication, ensure the following prerequisites are met:

1. **Unique Key/Index**: The table you wish to replicate must have a unique key or unique index. If this is not already the case, run the following command to enable it:
   ```
   ALTER TABLE table_name REPLICA IDENTITY FULL;
   ```

2. **Network Access**: The target database must have network access to the source database. Ensure that the security group allows the IP addresses of the target database.

## Steps

Follow these steps to set up replication:

1. **Create Publication (Source DB)**: Execute the following command on the source database to create a publication. In this example, we are replicating the "users" and "departments" tables. Replace the table names with your desired tables.
   ```
   CREATE PUBLICATION mypublication FOR TABLE users, departments;
   ```

2. **Create Subscription (Target DB)**: On the target database, execute the following command to create a subscription and connect it to the publication on the source database. Replace the connection string with the appropriate values for your setup.
   ```
   CREATE SUBSCRIPTION mysub CONNECTION 'host=192.168.1.50 port=5432 user=foo dbname=foodb' PUBLICATION mypublication;
   ```

3. **Airflow for Monitoring**: We recommend using Airflow for monitoring the synchronization and schema changes between databases. The following scripts are provided for this purpose:
   - `logical_replication_sync_check.py`: Monitors the synchronization between databases.
   - `logical_replication_schema_fix.py`: Fixes the synchronization issues when schema changes occur, such as dropping or adding columns.

## Monitoring

Airflow is a powerful platform for managing workflow orchestration and monitoring data pipelines. It provides a user-friendly interface for tracking the replication process and schema changes.

To use Airflow, follow these steps:

1. Install and set up Airflow on your system.

2. Create a DAG (Directed Acyclic Graph) to define your replication workflow, including the sync check and schema fix scripts.

3. Schedule the DAG to run at regular intervals to monitor and manage the replication process.

## Contributions

If you find any issues or have suggestions for improvements, feel free to contribute to this repository. We welcome contributions from the community to make this replication and monitoring process even better!

## License

This project is licensed under the [MIT License](LICENSE).

---
_This Readme template was created with love by ChatGPT. Let me know if you need any further assistance!_
