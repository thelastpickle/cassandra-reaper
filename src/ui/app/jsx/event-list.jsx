import React from "react";
import $ from "jquery";
import moment from "moment";
import _datatables from "datatables.net";
import _datatables_bs from "datatables.net-bs.js";


const diagEventsList = React.createClass({

  propTypes: {
    diagnosticEvents: React.PropTypes.object.isRequired,
    listenSubscriptionSubject: React.PropTypes.object.isRequired
  },

  getInitialState: function() {
    return {table: null};
  },

  componentWillMount: function() {

    this._listenSubscription = this.props.listenSubscriptionSubject.subscribeOnNext(subscription => {
      if(this.state.table) {
        this.state.table.clear();
      }
    });

    this._eventsSubscription = this.props.diagnosticEvents.subscribeOnNext(event => {
      if(!this.state.table) {
        const table = $('#eventsDataTable').DataTable({
           columns: [
               {
                 "className":      'details-control',
                 "orderable":      false,
                 "data":           null,
                 "defaultContent": '<span class="fa fa-search"></span> '
               },
               { title: 'Time' },
               { title: 'Cluster' },
               { title: 'Node' },
               { title: 'Class' },
               { title: 'Type' }
           ],
           pageLength: 25,
           order: [[1, 'asc']]
         });
         this.state.table = table;

         $('#eventsDataTable tbody').on('click', 'td.details-control', function () {
           const tr = $(this).closest('tr');
           const row = table.row(tr);
           if (row.child.isShown()) {
             row.child.hide();
             tr.removeClass('shown');
           } else {
             const data = row.data();
             const json = JSON.stringify(data, null, 2);
             row.child(`<div class="well well-sm">${json}</div>`).show();
             tr.addClass('shown');
           }
         });
      }
      const e = event.eventData;
      const cs = e.eventClass.split('.');
      this.state.table.row.add([e, moment(e.timestamp).format('LTS'), e.cluster, e.node, cs[cs.length-1], e.eventType]).draw(false);
    });
  },

  componentWillUnmount: function() {
    this._eventsSubscription.dispose();
    this._listenSubscription.dispose();
  },

  render: function() {
    return <table id="eventsDataTable" className="display table table-bordered" width="100%"/>;
  }
});

export default diagEventsList;