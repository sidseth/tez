/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

App.Helpers.misc = {
  getStatusClassForEntity: function(dag) {
    var st = dag.get('status');
    switch(st) {
      case 'FAILED':
        return 'failed';
      case 'KILLED':
        return 'killed';
      case 'RUNNING':
        return 'running';
      case 'ERROR':
        return 'error';
      case 'SUCCEEDED':
        var counterGroups = dag.get('counterGroups');
        var numFailedTasks = this.getCounterValueForDag(counterGroups,
          dag.get('id'), 'org.apache.tez.common.counters.DAGCounter',
          'NUM_FAILED_TASKS'
        ); 

        if (numFailedTasks > 0) {
          return 'warning';
        }

        return 'success';
      default:
        return 'submitted';
    }
  },

	getCounterValueForDag: function(counterGroups, dagID, counterGroupName, counterName) {
		if (!counterGroups) {
			return 0;
		}

		var cgName = dagID + '/' + counterGroupName;
		var cg = 	counterGroups.findBy('id', cgName);
		if (!cg) {
			return 0;
		}
		var counters = cg.get('counters');
		if (!counters) {
			return 0;
		}
		
		var counter = counters.findBy('id', cgName + '/' + counterName);
		if (!counter) return 0;

		return counter.get('value');
	},

  isValidDagStatus: function(status) {
    return $.inArray(status, ['SUBMITTED', 'INITING', 'RUNNING', 'SUCCEEDED',
      'KILLED', 'FAILED', 'ERROR']) != -1;
  },

  isValidTaskStatus: function(status) {
    return $.inArray(status, ['RUNNING', 'SUCCEEDED', 'FAILED', 'KILLED']) != -1;
  },

  /**
   * To trim a complete class path with namespace to the class name.
   */
  getClassName: function (classPath) {
    return classPath.substr(classPath.lastIndexOf('.') + 1);
  },

  /*
   * Normalizes counter style configurations
   * @param counterConfigs Array
   * @return Normalized configurations
   */
  normalizeCounterConfigs: function (counterConfigs) {
    return counterConfigs.map(function (configuration) {
      configuration.headerCellName = configuration.counterName || configuration.counterId;
      configuration.id = '%@/%@'.fmt(configuration.counterGroupName || configuration.groupId,
          configuration.counterName || configuration.counterId),
      configuration.getCellContent = App.Helpers.misc.getCounterCellContent;
      return configuration;
    });
  },

  /*
   * Creates column definitions form configuration object array
   * @param columnConfigs Array
   * @return columnDefinitions Array
   */
  createColumnsFromConfigs: function (columnConfigs) {
    return columnConfigs.map(function (columnConfig) {
      if(columnConfig.getCellContentHelper) {
        columnConfig.getCellContent = App.Helpers.get(columnConfig.getCellContentHelper);
      }
      columnConfig.minWidth = columnConfig.minWidth || 135;

      return columnConfig.filterID ?
          App.ExTable.ColumnDefinition.createWithMixins(App.ExTable.FilterColumnMixin, columnConfig) :
          App.ExTable.ColumnDefinition.create(columnConfig);
    });
  },

  /*
   * Returns a counter value from for a row
   * @param row
   * @return value
   */
  getCounterCellContent: function (row) {
    var contentPath = this.id.split('/'),
        group = contentPath[0],
        counter = contentPath[1],
        id = row.get('id'),
        value = 'Not Available';

    try{
      value = row.get('counterGroups').
          findBy('id', '%@/%@'.fmt(id, group)).
          get('counters').
          findBy('id', '%@/%@/%@'.fmt(id, group, counter)).
          get('value');
    }catch(e){}

    return App.Helpers.number.formatNumThousands(value);
  },

  /* 
   * returns a formatted message, the real cause is unknown and the error object details
   * depends on the error cause. the function tries to handle ajax error or a native errors
   */
  formatError: function(error, defaultErrorMessage) {
    var msg;
    // for cross domain requests, the error is not set if no access control headers were found.
    // this could be either because there was a n/w error or the cors headers being not set.
    if (error.status === 0 && error.statusText === 'error') {
      msg = defaultErrorMessage ;
    } else {
      msg = error.statusText || error.message;
    }
    msg = msg || 'Unknown error';
    if (!!error.responseText) {
      msg += error.responseText;
    }
    return {
      errCode: error.status || 'Unknown', 
      msg: msg,
      details: error.stack
    };
  },

  merge: function objectMerge(obj1, obj2) {
    $.each(obj2, function (key, val) {
      if(Array.isArray(obj1[key]) && Array.isArray(val)) {
        $.merge(obj1[key], val);
      }
      else if($.isPlainObject(obj1[key]) && $.isPlainObject(val)) {
        objectMerge(obj1[key], val);
      }
      else {
        obj1[key] = val;
      }
    });
  },

  dagStatusUIOptions: [
    { label: 'All', id: null },
    { label: 'Submitted', id: 'SUBMITTED' },
    { label: 'Running', id: 'RUNNING' },
    { label: 'Succeeded', id: 'SUCCEEDED' },
    { label: 'Failed', id: 'FAILED' },
    { label: 'Killed', id: 'KILLED' },
    { label: 'Error', id: 'ERROR' },
  ],

  vertexStatusUIOptions: [
    { label: 'All', id: null },
    { label: 'Running', id: 'RUNNING' },
    { label: 'Succeeded', id: 'SUCCEEDED' },
    { label: 'Failed', id: 'FAILED' },
    { label: 'Killed', id: 'KILLED' },
    { label: 'Error', id: 'ERROR' },
  ],

  taskStatusUIOptions: [
    { label: 'All', id: null },
    { label: 'Running', id: 'RUNNING' },
    { label: 'Succeeded', id: 'SUCCEEDED' },
    { label: 'Failed', id: 'FAILED' },
    { label: 'Killed', id: 'KILLED' },
  ],

  defaultQueryParamsConfig: {
    refreshModel: true,
    replace: true
  }

}
