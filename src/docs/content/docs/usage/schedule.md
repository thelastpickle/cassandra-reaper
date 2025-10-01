---
title: "Scheduling a Cluster Repair"
weight: 40
identifier: "schedules"
parent: "usage"
---

Reaper has the ability to create and manage repair schedules for a cluster. This can be done in the following way.

## Set up a Repair Schedule

Click the *schedule* menu item on the left side to navigate to the Schedules page. Click *Add schedule* to open the schedule details form.

{{< screenshot src="/img/schedule.png" />}}

<br/>

## Fill in the Details

Enter values for the keyspace, tables, owner and other fields and click *Add Schedule* button. The details for adding a schedule are similar to the details for [Repair](../single) form except the "Cause" field is replaced with three fields; "Start time", "Interval in days" and "Percent unrepaired threshold". See the table below for further information the two fields.

{{< screenshot src="/img/add_schedule.png" />}}

<br/>

| Option                           | Description                                                                                                                                                                                                                                             |
|----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Start time**                   | The time to trigger repairs, based in GMT.                                                                                                                                                                                                              |
| **Interval in days**             | The frequency for the schedule to be run.                                                                                                                                                                                                               |
| **Percent unrepaired threshold** | *For incremental repair only!* Sets the percentage of unrepaired data over which a repair run will be started for this schedule. As soon as one table in the set of tables managed by this schedule gets over the threshold, the run will be triggered. |

<br/>

After creating a scheduled repair, the page is updated with a list of active and paused repair schedules.

Note that when choosing to add a new repair schedule, it is recommended to restrict the repair schedules to specific tables, instead of scheduling repairs for an entire keyspace. Creating different repair schedules will allow for simpler scheduling, fine-grain tuning for more valuable data, and easily grouping tables with smaller data load into different repair cycles.

For example, if there are certain tables that contain valuable data or a business requirement for high consistency and high availability, they could be schedule to be repaired during low traffic periods.

Note that scheduled repairs can be paused and deleted by users with access to the Reaper web interface. To add authentication security the web UI see the [authentication](/docs/configuration/authentication) section for further information.
