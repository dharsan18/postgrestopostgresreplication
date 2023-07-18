# postgrestopostgresreplication
CDC native logical replication and monitoring



### prerequirment
- Table which you are replicating should have unique key or unique index
- Otherwise to make it work run Alter table table_name replica identity full;
- Network access should allow from target db to source db. So, please alter the security group IP's

## steps
- (source db)
        CREATE PUBLICATION mypublication FOR TABLE users, departments; 
- (target db)
        CREATE SUBSCRIPTION mysub
        CONNECTION 'host=192.168.1.50 port=5432 user=foo dbname=foodb' ## Ideally, this content should be read from a file since password is visible in system table
        PUBLICATION mypublication; 
- (airflow for monitoring the syncs / schema changes updates)
        - logical_replication_sync_check.py will monitor the sync between db's
        - logical_replication_schema_fix.py will fix the sync when schema changes happens   like dropping/adding columns