## From Raw Bus Data to a Real-Time Lakehouse: How I Built a Streaming Data Pipeline for Helsinki Public Transport

In September, over just two days, I processed around 2 million real-time events from Helsinki’s public transport system. This made it possible to monitor, with sub-minute latency, how many buses were on the road, what the average delays were, and much more.

The same data also powered deeper analysis, from finding the most popular routes to spotting the busiest transport hubs and other key insights.

All of this ran without losing a single event, stayed fully scalable, and, most importantly, running the pipeline for an entire month would cost less than one dollar.

Here’s how I built it.

---

### The Challenge

Helsinki’s public transport system (HSL) publishes a continuous MQTT feed of vehicle events. Thousands of messages flow in every minute:

* Where each bus is right now
* How late or early it is
* Which route it belongs to

The data is free — but it arrives fast, messy, and unstructured. If you want insights, you need to:

1. Ingest it in real time
2. Store it reliably
3. Process it into something useful
4. Analyze it interactively

And ideally, you want this system to run on a student budget, able to grow over time without costing more than a few dollars per month.

---

### Architecture at a Glance

I used a Medallion architecture with three layers:

* Bronze: Raw, untouched data directly from Kafka
* Silver: Cleaned and structured data
* Gold: Curated tables for analytics and dashboards

Here’s the stack I chose:

* Kafka for real-time ingestion
* Spark Structured Streaming for processing both real-time and batch data
* Delta Lake on Amazon S3 for storage (ACID transactions, schema evolution)
* Hive Metastore + Trino for SQL queries
* Airflow for orchestrating ETL jobs
* Prometheus + Grafana for real-time metrics
* Docker Compose for easy local deployment

![architecture](/docs/architecture.png)

The entire pipeline runs on my laptop + a tiny S3 bucket. Cost for 2M events? Less than $0.05.

---

### Making It Real-Time

The real-time part was the most intresting.

This Spark job listens to Kafka, aggregates events in 5-minute windows (sliding every 30 s), and publishes fresh KPIs to Prometheus every 15 s. Grafana then reads them from Prometheus and updates with sub-minute latency.

Start a tiny HTTP server and define gauges:

```python
from prometheus_client import Gauge, start_http_server
start_http_server(int(os.getenv("PROM_PORT", "9108")))

TOTAL_ACTIVE = Gauge("total_active_vehicles", "Active vehicles")
AVG_SPEED_ALL = Gauge("avg_speed_all", "Average speed (km/h)")
PCT_ON_TIME_1 = Gauge("pct_routes_on_time_1", "≤1 min")
PCT_ON_TIME_2 = Gauge("pct_routes_on_time_2", "≤2 min")
PCT_ON_TIME_3 = Gauge("pct_routes_on_time_3", "≤3 min")

ROUTE_DELAY_EXTREME = Gauge(
    "route_delay_extreme_seconds",
    "Avg delay extremes in the latest window",
    ["type","route_id"]
)
```

Read from Kafka and parse only what we need:

```python
kafka_stream = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers","kafka:29092")
    .option("subscribe","hsl_stream")
    .option("startingOffsets","latest")
    .load())

parsed_df = kafka_stream.selectExpr("CAST(value AS STRING) AS v").select(
    F.get_json_object("v","$.topic.route_id").alias("route_id"),
    F.get_json_object("v","$.topic.vehicle_number").alias("vehicle_number"),
    F.get_json_object("v","$.payload.VP.tst").alias("tst"),
    F.get_json_object("v","$.payload.VP.spd").cast("double").alias("speed"),
    F.get_json_object("v","$.payload.VP.dl").cast("double").alias("delay")
).withColumn("timestamp", F.to_timestamp("tst"))
```

Aggregate in sliding windows with a 1-minute watermark (the watermark could be set much higher if events arrive late, but in this case 1 minute was enough to reduce system and memory load while still keeping processing nearly real-time):

```python
windowed_df = (parsed_df
    .withWatermark("timestamp","1 minutes")
    .groupBy(F.window("timestamp","5 minutes","30 seconds"))
    .agg(
        F.approx_count_distinct("vehicle_number").alias("active_vehicles"),
        F.avg("speed").alias("avg_speed"),
        F.avg(F.when(F.abs("delay") <= 60, 1).otherwise(0)).alias("on_time_1"),
        F.avg(F.when(F.abs("delay") <= 120,1).otherwise(0)).alias("on_time_2"),
        F.avg(F.when(F.abs("delay") <= 180,1).otherwise(0)).alias("on_time_3"),
    )
    .select(F.col("window.end").alias("window_end"),
            "active_vehicles","avg_speed","on_time_1","on_time_2","on_time_3"))
```

Push only the freshest closed window to Prometheus:

```python
def update_prometheus(batch_df, batch_id):
    if batch_df.rdd.isEmpty(): return
    latest_end = batch_df.agg(F.max("window_end")).first()[0]
    row = (batch_df.filter(F.col("window_end")==latest_end)
                  .limit(1).collect()[0])

    TOTAL_ACTIVE.set(int(row["active_vehicles"] or 0))
    AVG_SPEED_ALL.set(float(row["avg_speed"] or 0.0))
    PCT_ON_TIME_1.set(float(row["on_time_1"] or 0.0))
    PCT_ON_TIME_2.set(float(row["on_time_2"] or 0.0))
    PCT_ON_TIME_3.set(float(row["on_time_3"] or 0.0))
```

Optional route extremes (best and worst average delay in the latest window):

```python
route_windowed_df = (parsed_df
    .withWatermark("timestamp","1 minutes")
    .groupBy(F.window("timestamp","5 minutes","30 seconds"), "route_id")
    .agg(F.avg("delay").alias("avg_delay"), F.count("*").alias("events_cnt"))
    .select(F.col("window.end").alias("window_end"),"route_id","avg_delay","events_cnt"))

def update_route_extremes(batch_df, batch_id):
    if batch_df.rdd.isEmpty(): return
    latest_end = batch_df.agg(F.max("window_end")).first()[0]
    latest = batch_df.filter(F.col("window_end")==latest_end).filter("events_cnt >= 5")
    if latest.rdd.isEmpty(): return
    ROUTE_DELAY_EXTREME.clear()
    worst = latest.orderBy(F.desc("avg_delay")).limit(1).collect()[0]
    best  = latest.orderBy(F.asc("avg_delay")).limit(1).collect()[0]
    ROUTE_DELAY_EXTREME.labels("worst", worst["route_id"] or "unknown").set(float(worst["avg_delay"] or 0.0))
    ROUTE_DELAY_EXTREME.labels("best",  best["route_id"]  or "unknown").set(float(best["avg_delay"] or 0.0))
```

Start both streams with 15-second triggers and separate checkpoints:

```python
(windowed_df.writeStream.outputMode("append")
 .foreachBatch(update_prometheus)
 .option("checkpointLocation","/home/jobs/checkpoint_data/realtime/metrics_totals")
 .trigger(processingTime="15 seconds")
 .start())

(route_windowed_df.writeStream.outputMode("append")
 .foreachBatch(update_route_extremes)
 .option("checkpointLocation","/home/jobs/checkpoint_data/realtime/metrics_extremes")
 .trigger(processingTime="15 seconds")
 .start()
 .awaitTermination())
```

What you see in Grafana:

![grafana live metrics](/docs/grafana_metrics.png)

Total active vehicles, average speed, on-time ratios (≤1/2/3 min), and the current best and worst routes by average delay update in near real time.


---

### Historical Analytics

For deeper analysis, Airflow triggers batch jobs that transform data into the Gold layer using Slowly Changing Dimensions (SCD2) for routes and stops.

![airflow](/docs/airflow.png)

This keeps full history:

* If a bus stop moves or changes name -> the old version is preserved
* If a route gets rebranded -> both old and new versions exist in the data

With Trino + SQL, I could ask questions like:

* Which routes had the most delays?
* Where are the busiest stops?
* How many buses run per route per day?

---

### Favorite Insights (2 days time window)

* Peak traffic: 7–9 AM and 3–6 PM with ~890 buses simultaneously on the road

![buses](/docs/vehicles.png)

* On-time arrival performance:

  * Within 3 min: About 80–85% of buses arrive on time
  * Within 2 min: Around 70–75% stay within two minutes of schedule
  * Within 1 min: About 40–50% manage to stay within one minute

![delays](/docs/delays.png)

* Most popular route: Route 611 with 45 buses in one day

![bus_611_table](/docs/611_table.png)

![bus_611_map](/docs/611_map.png)

* Biggest stop hub: Helsinki Central Railway Station

![hubs_table](/docs/hubs_route.png)

![hubs_map](/docs/hubs_map.png)

I also discovered Route 99V — a temporary metro replacement bus with very high frequency (~every 2.5 min) due to construction work.

```sql
SELECT 
    f.oday,
    f.route,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    COUNT(DISTINCT f.vehicle_number) AS bus_count,
    COUNT(DISTINCT f.stop) AS unique_stops,
    ROUND(
        CAST(COUNT(DISTINCT f.vehicle_number) AS DOUBLE) 
        / CAST(COUNT(DISTINCT f.stop) AS DOUBLE),
      2
    ) AS veh_to_stops_ratio
FROM hdw.fact_vehicle_position f
JOIN hdw.dim_routes r
    ON f.route_id = r.route_id
    AND r.active_flg = 1
WHERE f.oday = DATE '2025-09-08'
GROUP BY f.oday, f.route_id, f.route, r.route_short_name, r.route_long_name, r.route_type
ORDER BY veh_to_stops_ratio DESC
LIMIT 10;
```

![99v](/docs/99v.png)

![99v_map](/docs/99v_map.png)

[Source](https://www.hsl.fi/en/hsl/news/service-updates/2025/03/no-metro-services-to-vuosaari-or-rastila-from-5-may--we-will-increase-bus-services-in-the-area)


---

### Why It Matters

This project shows how open-source tools can deliver both:

* Operational dashboards -> live insights for dispatchers and planners
* Historical analytics -> long-term performance trends

All at almost zero cost and fully scalable to tens of millions of events per day with Kubernetes or cloud clusters.

---

### Next steps if someone wants to extend the project

For anyone looking to build on top of this pipeline, the most valuable additions would be:

- Adding trams, metro, and trains to increase coverage
- Adding a simple Kafka connector to save incoming events as raw files and then load the landing layer from these files instead of directly from the Kafka topic — this provides an additional way to store and replay data if needed
- Deploying on Kubernetes to scale Spark nodes for larger workloads

---

### Final Thoughts

Building this project was like watching a city breathe in real time. One moment, you see the rush hour chaos; the next, the calm of 3 AM when only a handful of buses are running.

And the best part? It’s all powered by open-source tech, a bit of cloud storage, and a lot of curiosity.

---

If you want the full technical deep-dive, including architecture diagrams, SQL queries, and Docker setup, check out the [GitHub repo](https://github.com/oiivantsov/transport-streaming-lakehouse).