# Frequently Asked Questions


### Why use Reaper instead of noddetool + cron?


While it's possible to set up crontab to call nodetool, it requires staggering the crons to ensure overlap is kept to a minimum.  Reaper is able to intelligently schedule repairs to avoid putting too much load on the cluster, avoiding impacting performance.  Reaper also offers a simple UI to schedule repairs as granularly as needed.


### Do I need to do repairs if I'm not deleting data?

Yes!  Repair is a necessary anti-entropy mechanism that keeps your cluster consistent.  Without repair, queries at LOCAL_ONE could return incorrect results.  


### Why are there four backends?  Which should i use?

When we (The Last Pickle) took over development of Reaper, we found it cumbersome to require a PostGres database in addition to the Cassandra database.  We also knew Reaper would need to be fault tolerant and work across datacenters.  The most straightforward way to do this would be to leverage Cassandra's fault tolerance.  

For small setups, using a local DB (H2) can make sense.  This would allow reaper to run on a single node using EBS for storage.  This is intentionally a very simple design to keep costs down and avoid extra operational work.

For new installations, we're recommending the Cassandra backend as it allows the most flexibility, with the highest level of availability.
