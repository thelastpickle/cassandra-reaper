# Frequently Asked Questions


### Why use Reaper instead of nodetool + cron?

While it's possible to set up crontab to call nodetool, it requires staggering the crons to ensure overlap is kept to a minimum.  Reaper is able to intelligently schedule repairs to avoid putting too much load on the cluster, avoiding impacting performance.  Reaper also offers a simple UI to schedule repairs as granularly as needed.


### Do I need to do repairs if I'm not deleting data?

Yes!  Repair is a necessary anti-entropy mechanism that keeps your cluster consistent.  Without repair, queries at LOCAL_ONE could return incorrect results.  


### Which backend is used to store Reaper's data?

When we (The Last Pickle) took over development of Reaper, we found it cumbersome to require a PostGres database in addition to the Cassandra database.  We also knew Reaper would need to be fault tolerant and work across datacenters.  The most straightforward way to do this would be to leverage Cassandra's fault tolerance.  

Postgres and H2 were removed starting with v3.0.0 in order to simplify the codebase and feature addition.
The memory backend is still available for testing purposes.