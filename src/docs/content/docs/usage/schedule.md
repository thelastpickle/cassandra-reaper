+++
[menu.docs]
name = "Scheduling a Cluster Repair"
weight = 40
identifier = "schedules"
parent = "usage"
+++

# Scheduling a Cluster Repair

Reaper has the ability to create and manage repair schedules for a cluster. This can be done in the following way.

## Setup a Repair Schedule

Click the *schedule* menu item on the left side to navigate to the Schedules page. Click *Add schedule* to open the schedule details form.

{{< screenshot src="/img/schedule.png" />}}

<br/>

## Fill in the Details

Enter values for the keyspace, tables, owner and other fields and click *Add Schedule* button. The details for adding a schedule are similar to the details for [Repair](../single) form except the "Clause" field is replaced with two fields; "Start time" and "Interval in days". See the table below for further information the two fields.

{{< screenshot src="/img/add_schedule.png" />}}

<br/>

<h4>Option</h4> | <h4>Description</h4>
---|---
**Start time** | The time to trigger repairs, based in GMT.
**Interval in days** | The frequency for the schedule to be run.

<br/>

After creating a scheduled repair, the page is updated with a list of active and paused repair schedules.

Note that when choosing to add a new repair schedule, it is recommended to restrict the repair schedules to specific tables, instead of scheduling repairs for an entire keyspace. Creating different repair schedules will allow for simpler scheduling, fine-grain tuning for more valuable data, and easily grouping tables with smaller data load into different repair cycles.

For example, if there are certain tables that contain valuable data or a business requirement for high consistency and high availability, they could be schedule to be repaired during low traffic periods.

Note that scheduled repairs can be paused and deleted by users with access to the Reaper web interface. To add authentication security the web UI see the [authentication](/docs/configuration/authentication) section for further information.
