//
//  Copyright 2018-2018 Stefan Podkowinski
//  Copyright 2019-2019 The Last Pickle Ltd
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

import React from "react";
import CreateReactClass from 'create-react-class';
import PropTypes from 'prop-types';
import $ from "jquery";
import moment from "moment";
import _datatables from "datatables.net";
import _datatables_bs from "datatables.net-bs.js";


const diagEventsList = CreateReactClass({

  propTypes: {
    diagnosticEvents: PropTypes.object.isRequired,
    listenSubscriptionSubject: PropTypes.object.isRequired,
    clusterSelected: PropTypes.object.isRequired,
  },

  getInitialState: function() {
    return {table: null};
  },

  UNSAFE_componentWillMount: function() {

    this._listenSubscription = this.props.listenSubscriptionSubject.subscribeOnNext(this._clear);
    this._clusterSelectedSubscription = this.props.clusterSelected.subscribeOnNext(this._clear);

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
           order: [[1, 'desc']]
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
             row.child(`<div class="well well-sm"><pre><code>${json}</code></pre></div>`).show();
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
    this._clusterSelectedSubscription.dispose();
  },

  render: function() {
    return <table id="eventsDataTable" className="display table table-bordered" width="100%"/>;
  },

  _clear: function() {
    if(this.state.table) {
      this.state.table.clear();
    }
  }
});

export default diagEventsList;
