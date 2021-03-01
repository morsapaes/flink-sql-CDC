# Change Data Capture with Flink SQL and Debezium

See the [slides](https://noti.st/morsapaes/liQzgs/change-data-capture-with-flink-sql-and-debezium) for context.

## Docker

To keep things simple, this demo uses a Docker Compose setup that makes it easier to bundle up all the services you need for your CDC pipeline:

<p align="center">
<img width="700" alt="demo_overview" src="https://user-images.githubusercontent.com/23521087/90702325-3c67f980-e28b-11ea-8496-9f237ceeae8b.png">
</p>

#### Getting the setup up and running
`docker-compose build`

`docker-compose up -d`

#### Is everything really up and running?

`docker-compose ps`

You should be able to access the Flink Web UI (http://localhost:8081), as well as Kibana (http://localhost:5601).

## Postgres

Start the Postgres client to have a look at the source tables and run some DML statements later:

`docker-compose exec postgres env PGOPTIONS="--search_path=claims" bash -c 'psql -U $POSTGRES_USER postgres'`

#### What tables are we dealing with?

```sql
SELECT * FROM information_schema.tables WHERE table_schema='claims';
```

## Debezium

Start the [Debezium Postgres connector](https://debezium.io/documentation/reference/1.2/connectors/postgresql.html) using the configuration provided in the `register-postgres.json` file:

```bash
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-postgres.json
```

Check that the connector is running:

```bash
curl http://localhost:8083/connectors/claims-connector/status # | jq
```

The first time it connects to a Postgres server, Debezium takes a [consistent snapshot](https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-snapshots) of all database schemas; so, you should see that the pre-existing records in the `accident_claims` table have already been pushed into your Kafka topic:

```bash
docker-compose exec kafka /kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --from-beginning \
    --property print.key=true \
    --topic pg_claims.claims.accident_claims
```

> ℹ️ Have a quick read about the structure of these events in the [Debezium documentation](https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-change-events-value).

### Is it working?

In the tab you used to start the Postgres client, you can now run some DML statements to see that the changes are propagated all the way to your Kafka topic:

```sql
INSERT INTO accident_claims (claim_total, claim_total_receipt, claim_currency, member_id, accident_date, accident_type, accident_detail, claim_date, claim_status) VALUES (500, 'PharetraMagnaVestibulum.tiff', 'AUD', 321, '2020-08-01 06:43:03', 'Collision', 'Blue Ringed Octopus', '2020-08-10 09:39:31', 'INITIAL');
```

```sql
UPDATE accident_claims SET claim_total_receipt = 'CorrectReceipt.pdf' WHERE claim_id = 1001;
```

```sql
DELETE FROM accident_claims WHERE claim_id = 1001;
```

In the output of your Kafka console consumer, you should now see three consecutive events with `op` values equal to `c` (an _insert_ event), `u` (an _update_ event) and `d` (a _delete_ event).

## Flink SQL

Start the Flink SQL Client:

```bash
docker-compose exec sql-client ./sql-client.sh
```

Register a [Postgres catalog](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/jdbc.html#postgres-database-as-a-catalog), so you can access the metadata of the external tables over JDBC:

```sql
CREATE CATALOG postgres WITH (
    'type'='jdbc',
    'property-version'='1',
    'base-url'='jdbc:postgresql://postgres:5432/',
    'default-database'='postgres',
    'username'='postgres',
    'password'='postgres'
);
```

Create a changelog table to consume the change events from the `pg_claims.claims.accident_claims` topic, with the same schema as the `accident_claims` source table, that consumes the `debezium-json` format:

```sql
CREATE TABLE accident_claims
WITH (
  'connector' = 'kafka',
  'topic' = 'pg_claims.claims.accident_claims',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'test-consumer-group',
  'format' = 'debezium-json',
  'scan.startup.mode' = 'earliest-offset'
 )
LIKE postgres.postgres.`claims.accident_claims` ( 
EXCLUDING OPTIONS);
```

and register a reference `members` table:

```sql
CREATE TABLE members
LIKE postgres.postgres.`claims.members` ( 
INCLUDING OPTIONS);
```

> ℹ️ For more details on the Debezium Format, refer to the [Flink documentation](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/connectors/formats/debezium.html#debezium-format).

### Ch-ch-ch-ch-changes

You can now query the changelog source table you just created:

```sql
SELECT * FROM accident_claims;
```

and (same as [before](#is-it-working)) insert, update and delete a record in the Postgres client to see how the query results update in (near) real-time.

> ℹ️ If you're curious to see what's going on behind the scenes (i.e. how queries translate into continuously running Flink jobs) check the Flink Web UI (http://localhost:8081).

## Visualizing the Results

Create a sink table that will write to a new `agg_insurance_costs` index on Elasticsearch:

```sql
CREATE TABLE agg_insurance_costs (
  es_key STRING PRIMARY KEY NOT ENFORCED,
  insurance_company STRING,
  accident_detail STRING,
  accident_agg_cost DOUBLE
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://elasticsearch:9200',
  'index' = 'agg_insurance_costs'
);
```

and submit a continuous query to the Flink cluster that will write the aggregated insurance costs per `insurance_company`, bucketed by `accident_detail` (or, what animals are causing the most harm in terms of costs):

```sql
INSERT INTO agg_insurance_costs
SELECT UPPER(SUBSTRING(m.insurance_company,0,4) || '_' || SUBSTRING (ac.accident_detail,0,4)) es_key,
       m.insurance_company,
       ac.accident_detail,
       SUM(ac.claim_total) member_total
FROM accident_claims ac
JOIN members m
ON ac.member_id = m.id
WHERE ac.claim_status <> 'DENIED'
GROUP BY m.insurance_company, ac.accident_detail;
```

Finally, create a simple [dashboard in Kibana](https://www.elastic.co/guide/en/kibana/current/dashboard-create-new-dashboard.html) with a 1s refresh rate and use the (very rustic) `postgres_datagen.sql` data generator script to periodically insert some records into the Postgres source table, creating visible changes in your results:

```bash
cat ./postgres_datagen.sql | docker exec -i flink-sql-cdc_postgres_1 psql -U postgres -d postgres
```

![flink-sql-CDC_kibana](https://user-images.githubusercontent.com/23521087/109538607-93fbe300-7ac0-11eb-8840-2ed7b2aafa27.gif)

<hr>

**And that's it!**

This demo will get some polishing as CDC support in Flink matures.

If you have any questions or feedback, feel free to DM me on Twitter [@morsapaes](https://twitter.com/morsapaes).
