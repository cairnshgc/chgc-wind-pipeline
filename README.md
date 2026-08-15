# CHGC wind data pipeline

Pulls wind readings from the Cairns Hang Gliding Club's weather station at Rex Lookout
and files them into the club's long-term archive.

Run nightly, unattended, by the club's Google Cloud project. Costs about nothing.

**If you are a future club volunteer reading this cold: you almost certainly do not
need to touch any of it.** It looks after itself. This file exists so that if it ever
does break, or the club wants to change what it collects, whoever comes next has a
fighting chance.

---

## What it actually does

The weather station at Rex sends a reading every five minutes: wind speed, wind
direction, battery voltage and mobile signal strength. Those readings go to
**eagle.io**, a monitoring service run for us by CBased Environmental.

eagle.io shows the current conditions beautifully, but it is not ours, and we do not
know how long it keeps old readings. So every night this job asks eagle.io for the
last day and a half of readings and copies them into **BigQuery**, a database in the
club's own Google Cloud account, where they sit alongside nine years of history going
back to November 2017.

That archive is the raw material for the interesting things: what the wind actually
does at Rex month by month, when the flyable windows are, and how often they happen.

## The one thing worth understanding

**Running this job twice never does any harm.**

It does not blindly add whatever it fetches. It matches on timestamp and only inserts
readings that are not already there. So you can run it again, run it over dates you
already have, or panic and run it five times, and the archive does not change.

That is deliberate, and everything else leans on it. Each nightly run deliberately
re-fetches **36 hours** rather than 24, so the windows overlap. If one night fails,
the next night quietly covers the gap and nobody has to do anything.

## How it is wired together

Three dull, separate pieces, on purpose:

| Piece | What it is | Why |
|---|---|---|
| **Cloud Scheduler** `chgc-wind-pull-daily` | Cron in the cloud. 03:15 Brisbane, daily. | Its only job is to poke the next thing awake. |
| **Cloud Run job** `chgc-wind-pull` | Runs this script, then exits. | A *job*, not a server. Nothing is listening, so there is no URL to secure. |
| **Secret Manager** `eagle-api-key` | Holds the read-only eagle.io key. | The key is never in this repo. Only the pipeline's service account can read it. |

Region is `australia-southeast1` throughout, matching where the data lives.
Service account: `chgc-wind-pull@chgc-wind-data.iam.gserviceaccount.com`.

Destination table: `chgc-wind-data.chgc_wind.readings`, partitioned by day and
clustered by `source`. Rows from this job are tagged `source = 'eagle_api'`; the
historical import from the old Vista Data Vision console is tagged `vdv_csv`.

## Running it by hand

You need the `gcloud` command line tool, or just use Google Cloud Shell in a browser,
which has it already.

```bash
# See what it would do, without writing anything
gcloud run jobs execute chgc-wind-pull --region australia-southeast1 \
  --args="--dry-run" --wait

# Normal run (last 36 hours)
gcloud run jobs execute chgc-wind-pull --region australia-southeast1 --wait

# Fill in a specific period, e.g. after a long outage
gcloud run jobs execute chgc-wind-pull --region australia-southeast1 \
  --args="--start,2026-08-01T00:00:00Z" --wait

# What happened?
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="chgc-wind-pull"' \
  --limit=60 --order=asc --freshness=30m --format='value(jsonPayload.message)'
```

Times are UTC. Cairns is UTC+10 all year (Queensland has no daylight saving).

## Deploying a change

Edit, commit, push, then from a machine with `gcloud`:

```bash
git clone https://github.com/cairnshgc/chgc-wind-pipeline.git
cd chgc-wind-pipeline

gcloud run jobs deploy chgc-wind-pull \
  --source . \
  --region australia-southeast1 \
  --service-account chgc-wind-pull@chgc-wind-data.iam.gserviceaccount.com \
  --set-secrets EAGLE_API_KEY=eagle-api-key:latest \
  --task-timeout 15m --max-retries 1 --memory 512Mi
```

Then run it once with `--dry-run` before trusting it.

## If something goes wrong

An alert emails `flying@cairnshangglidingclub.org` when the job logs an error. The
email subject is generic; click **VIEW INCIDENT** for the actual reason.

Two quite different things can trigger it:

1. **The job broke.** The eagle.io API changed, the key was revoked, the network
   failed. The logs will say which.
2. **The job worked perfectly and there was no data.** This usually means the weather
   station itself is offline, which has happened before: it was dead for 119 days from
   October 2025 after a sensor failed. Cloud Run considers this a success, which is
   exactly why the script checks whether the archive is actually growing and complains
   if the newest reading is more than 48 hours old.

Either way, **check the public dashboard first** to see whether the station is alive:
https://public.eagle.io/public/dash/fxur3cyl5u9o1ef

Then re-run the job by hand. It is safe.

## Things that are not obvious

- **The archive is the asset; this job is disposable.** Deleting the Cloud Run job does
  not delete any data. It can be rebuilt from this repo in minutes.
- **Do not "improve" this to run every five minutes.** Anything needing live data
  should read eagle.io directly, because eagle always has the newest reading and
  BigQuery is only as fresh as the last pull. Running more often would generate a lot
  of logs nobody reads to solve a problem nobody has.
- **If the key is ever reissued**, add a new *version* to the `eagle-api-key` secret.
  The job reads `:latest`, so it picks it up with no redeploy.
- **Timestamps are stored in UTC.** The old console exports were in Cairns local time
  and had to be converted. Mixing the two silently is the single easiest way to corrupt
  this archive.

## Contact

Club: flying@cairnshangglidingclub.org
Weather station service: CBased Environmental
