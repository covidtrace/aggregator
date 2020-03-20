# worker

The worker repository holds source code to aggregate and transform CovidTrace location and testing data.

### Jobs

- Hourly (Once per hour)
  - Collect all pending `holding` CSVs into S2 geo buckets
- Self Reported Symptoms
  - Place individual CSV into `holding` bucket with some UUID
