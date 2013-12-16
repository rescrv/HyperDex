/* Copyright (c) 2013, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of HyperDex nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

var app = angular.module('hyperdexnoc', ['ui.bootstrap']);

app.factory('BackendPropertyService', function() {
  var BackendPropertyService = {};
  BackendPropertyService.url = 'http://localhost:5000';
  BackendPropertyService.set = function(url) {
    BackendPropertyService.url = url;
  };
  return BackendPropertyService;
});

var google_good = false;

function SetGoogleGood() {
  google_good = true;
};

app.factory('GoogleVisualizationService', function() {
  var GoogleVisualizationService = {};
  GoogleVisualizationService.setup = function() {
    return google_good;
  };
  return GoogleVisualizationService;
});

app.factory('ChartPropertiesService', ['$rootScope', '$http', 'BackendPropertyService',
        function ($rootScope, $http, BackendPropertyService) {
  var ChartPropertiesService = {};
  ChartPropertiesService.properties = [];
  ChartPropertiesService.fetch = function() {
    $http.jsonp(BackendPropertyService.url + '/properties.json?callback=JSON_CALLBACK', {timeout: 500})
      .success(function(data, status, headers, config) {
        ChartPropertiesService.properties = data;
      }).error(function(data, status, headers, config) {
        // XXX
      });
  };
  ChartPropertiesService.lookupProperty = function(prop) {
    for (var i = 0; i < ChartPropertiesService.properties.length; ++i) {
      if (ChartPropertiesService.properties[i].tag == prop) {
        return ChartPropertiesService.properties[i];
      }
    }
    return null;
  };
  ChartPropertiesService.fetch();
  return ChartPropertiesService;
}]);

app.factory('ChartRefreshService', ['$timeout',
        function ($timeout) {
  var ChartRefreshService = {};
  ChartRefreshService.timeout = null;
  ChartRefreshService.tick = 0;
  ChartRefreshService.tock = function() {
    $timeout.cancel(ChartRefreshService.timeout);
    ChartRefreshService.timeout = $timeout(ChartRefreshService.tock, 1000);
    ChartRefreshService.tick += 1;
  };
  ChartRefreshService.timeout = $timeout(ChartRefreshService.tock, 1000);
  return ChartRefreshService;
}]);

app.factory('ClusterConfigService', ['$http', '$timeout', 'BackendPropertyService',
        function ($http, $timeout, BackendPropertyService) {
  var ClusterConfigService = {};
  ClusterConfigService.config = {};
  ClusterConfigService.fetch = function() {
    $http.jsonp(BackendPropertyService.url + '/config.json?callback=JSON_CALLBACK', {timeout: 500})
      .success(function(data, status, headers, config) {
        if (!angular.equals(ClusterConfigService.config, data)) {
          ClusterConfigService.config = data;
        }
        $timeout.cancel(ClusterConfigService.timeout);
        ClusterConfigService.timeout = $timeout(ClusterConfigService.fetch, 1000);
      }).error(function(data, status, headers, config) {
        $timeout.cancel(ClusterConfigService.timeout);
        ClusterConfigService.timeout = $timeout(ClusterConfigService.fetch, 1000);
        // XXX
      });
  };
  ClusterConfigService.timeout = $timeout(ClusterConfigService.fetch, 1000);
  ClusterConfigService.fetch();
  return ClusterConfigService;
}]);

app.controller('BackendSelectorCtrl', ['$scope', 'BackendPropertyService',
        function ($scope, BackendPropertyService) {
  $scope.edit = false;

  $scope.toggle = function() {
    $scope.edit = true;
  };

  $scope.save = function() {
    $scope.edit = false;
    BackendPropertyService.set($scope.tmp.url);
  };

  $scope.reset = function() {
    $scope.url = BackendPropertyService.url;
    $scope.tmp = {url: BackendPropertyService.url};
  };

  $scope.reset();

  $scope.$watch(function() {
    return {url: BackendPropertyService.url};
  }, function() {
    $scope.reset();
  }, true);
}]);

app.controller('WidgetsDisplayCtrl', ['$scope', 'BackendPropertyService',
        function ($scope, BackendPropertyService) {
  $scope.widgets = [{template: "/partials/servers.html",
                     arg: [{category: "Messages",
                            units: "requests",
                            tag: "msgs.req_get",
                            name: "Request Get",
                            form: "aggregate"},
                           {category: "Messages",
                            units: "requests",
                            tag: "msgs.req_atomic",
                            name: "Request Atomic",
                            form: "aggregate"},
                           {category: "I/O",
                            units: "requests",
                            tag: "io.in_flight",
                            name: "I/Os In-Flight",
                            form: "instant"},
                           {category: "I/O",
                            units: "milliseconds",
                            tag: "io.time_in_queue",
                            name: "Time Spent on All Requests",
                            form: "aggregate"}]},
                    {template: "/partials/default_chart.html",
                     arg: {type: "line",
                           options: {hAxis: {title: "Time",
                                             format: "#",
                                             viewWindowMode: "maximized",
                                             allowContainerBoundaryTextCutoff: true,
                                             showTextEvery: 30},
                                     vAxis: {minValue: 0},
                                     vAxes: [{units: "requests",
                                              minValue: 0}],
                                     series: [{targetAxisIndex: 0},
                                              {targetAxisIndex: 0},
                                              {targetAxisIndex: 0}],
                                     pointSize: 4,
                                     enableInteractivity: false,
                                     animation: {duration: 250,
                                                 easing: "inAndOut"},
                                     title: "Read, Write, and Search Throughput"},
                           fetch: {duration: 60,
                                   interval: 1000,
                                   data: [{props: ["msgs.req_get"],
                                           group: "sum",
                                           label: "Read"},
                                          {props: ["msgs.req_atomic"],
                                           group: "sum",
                                           label: "Write"},
                                          {props: ["msgs.req_count", "msgs.req_group_del",
                                                   "msgs.req_search_describe",
                                                   "msgs.req_search_start",
                                                   "msgs.req_sorted_search"],
                                           group: "sum",
                                           label: "Search"}]}}},
                    {template: "/partials/default_chart.html",
                     arg: {type: "line",
                           options: {hAxis: {title: "Time",
                                             format: "#",
                                             viewWindowMode: "maximized",
                                             allowContainerBoundaryTextCutoff: true,
                                             showTextEvery:30},
                                     vAxis: {minValue:0},
                                     vAxes: [{units: "seconds", minValue: 0}],
                                     series: [{targetAxisIndex:0}],
                                     pointSize: 4,
                                     enableInteractivity: false,
                                     animation: {duration: 250,
                                                 easing: "inAndOut"},
                                     title: "LevelDB Statistics"},
                           fetch: {duration: 60,
                                   interval: 1000,
                                   data: [{props: ["leveldb.time0", "leveldb.time1",
                                                   "leveldb.time2", "leveldb.time3",
                                                   "leveldb.time4", "leveldb.time5"],
                                           group: "avg",
                                           label: "Compaction Time (Per Server)"}]}}}]

  $scope.remove = function(idx) {
    $scope.widgets.splice(idx, 1);
  };

  $scope.down = function(idx) {
    var tmp = $scope.widgets[idx];
    $scope.widgets[idx] = $scope.widgets[idx + 1];
    $scope.widgets[idx + 1] = tmp;
  };

  $scope.up = function(idx) {
    var tmp = $scope.widgets[idx - 1];
    $scope.widgets[idx - 1] = $scope.widgets[idx];
    $scope.widgets[idx] = tmp;
  };

  $scope.addChart = function() {
    $scope.widgets.push({template: '/partials/default_chart.html'});
  };

  $scope.addServers = function() {
    $scope.widgets.push({template: '/partials/servers.html'});
  };
}]);

/* This little controller renders the list of servers */
app.controller('ServerListCtrl', ['$scope', '$http', '$timeout', 'BackendPropertyService', 'ChartPropertiesService', 'ClusterConfigService',
        function ($scope, $http, $timeout, BackendPropertyService, ChartPropertiesService, ClusterConfigService) {
  $scope.sparklines = [];
  $scope.properties = [];
  $scope.servers = [];

  $scope.url = function() {
    return BackendPropertyService.url + '/config.json?callback=JSON_CALLBACK';
  };

  $scope.classes = function(server) {
    base = 'server-' + server.state;
    if (server.state == 'AVAILABLE') {
      base += ' success';
    }
    if (server.state == 'SHUTDOWN') {
      base += ' info';
    }
    if (server.state == 'NOT_AVAILABLE') {
      base += ' error';
    }
    return base;
  };

  $scope.removeSparkline = function(idx) {
    $scope.sparklines.splice(idx, 1);
  };

  $scope.$watch(function() {
    return {properties: ChartPropertiesService.properties};
  }, function() {
    $scope.properties = ChartPropertiesService.properties;
  }, true);
  $scope.$watch(function() {
    return {servers: ClusterConfigService.config.servers};
  }, function() {
    $scope.servers = ClusterConfigService.config.servers;
  }, true);

  $scope.shouldBeOpen = false;
  $scope.open = function () {
    $scope.shouldBeOpen = true;
  };
  $scope.close = function () {
    $scope.shouldBeOpen = false;
  };
  $scope.label = function(x) {
    return x.name + ' (' + x.units + (x.form == 'aggregate' ? '/second)' : ')');
  };

  $scope.expandServer = function(idx) {
    var chart = {};
    chart.type = 'line';
    chart.options = {hAxis: {title: 'Time',
                             format: '#',
                             viewWindowMode: 'maximized',
                             allowContainerBoundaryTextCutoff: true,
                             showTextEvery: 30},
                     vAxis: {minValue: 0},
                     vAxes: [],
                     series: [],
                     pointSize: 4,
                     enableInteractivity: false,
                     animation: {duration: 250, easing: 'inAndOut'}};
    chart.fetch = {duration: 60, interval: 1000, data: []};
    for (i = 0; i < $scope.sparklines.length; ++i) {
      chart.fetch.data.push({props: [$scope.sparklines[i].tag], label: $scope.sparklines[i].name,
                             group: 'server', group_opts: {id: $scope.servers[idx].id}});
    }
    $scope.widgets.push({template: '/partials/default_chart.html', arg: chart});
  };

  $scope.expandSparkline = function(idx) {
    var chart = {};
    chart.type = 'line';
    chart.options = {hAxis: {title: 'Time',
                             format: '#',
                             viewWindowMode: 'maximized',
                             allowContainerBoundaryTextCutoff: true,
                             showTextEvery: 30},
                     vAxis: {minValue: 0},
                     vAxes: [],
                     series: [],
                     pointSize: 4,
                     enableInteractivity: false,
                     animation: {duration: 250, easing: 'inAndOut'}};
    chart.fetch = {duration: 60, interval: 1000, data: []};
    for (i = 0; i < $scope.servers.length; ++i) {
      chart.fetch.data.push({props: [$scope.sparklines[idx].tag], label: $scope.servers[i].location,
                             group: 'server', group_opts: {id: $scope.servers[i].id}});
    }
    $scope.widgets.push({template: '/partials/default_chart.html', arg: chart});
  };

  $scope.init = function(arg) {
    if (typeof arg != 'undefined') {
      angular.copy(arg, $scope.sparklines);
    }
  };
}]);

app.controller('DefaultChartCtrl', ['$scope',
        function ($scope) {
  $scope.chart = {};
  $scope.chart.type = 'line';
  $scope.chart.options = {hAxis: {title: 'Time',
                                  format: '#',
                                  viewWindowMode: 'maximized',
                                  allowContainerBoundaryTextCutoff: true,
                                  showTextEvery: 30},
                          vAxis: {minValue: 0},
                          vAxes: [],
                          series: [],
                          pointSize: 4,
                          enableInteractivity: false,
                          animation: {duration: 250, easing: 'inAndOut'}};
  $scope.chart.fetch = {duration: 60, interval: 1000,
                        data: [{props: ['msgs.req_get', 'msgs.req_atomic'],
                                group: 'sum', label: 'Throughput'}]};
  $scope.shouldBeOpen = false;
  $scope.open = function () {
    $scope.shouldBeOpen = true;
  };
  $scope.close = function () {
    $scope.shouldBeOpen = false;
  };

  $scope.init = function(arg) {
    if (typeof arg != 'undefined') {
      angular.copy(arg, $scope.chart);
    }
  };
}]);

app.controller('SparklineChartCtrl', ['$scope',
        function ($scope) {
  $scope.chart = {};
  $scope.chart.type = 'stepped';
  $scope.chart.options = {chartArea: {width: '100%', height: '100%'},
                          hAxis: {textPosition: 'none', gridlines: {color: 'transparent'}},
                          vAxis: {minValue: 0, textPosition: 'none', gridlines: {color: 'transparent'}},
                          vAxes: [],
                          series: [],
                          pointSize: 1,
                          width: 100, height: 25,
                          enableInteractivity: false,
                          baselineColor: 'transparent',
                          legend: 'none'};
  $scope.chart.fetch = {duration: 10, interval: 1000, data: []};

  $scope.setup = function(prop, server) {
    if (prop) {
      $scope.chart.fetch.data = [{props: [prop], aggregate: 'sum', group: 'server', group_opts: {id: server}}];
    }
  };
}]);

app.controller('EditChartCtrl', ['$scope', 'ChartPropertiesService', 'ClusterConfigService',
        function ($scope, ChartPropertiesService, ClusterConfigService) {
  // the configuration info of the chart
  $scope.format = 'timeseries';
  $scope.types = [{type: 'bar', name: 'Horizontal Bar Chart'},
                  {type: 'column', name: 'Vertical Bar Chart'},
                  {type: 'line', name: 'Line Chart'}];
  $scope.properties = [];
  $scope.servers = [];
  $scope.groups = [{type: 'sum', name: 'Sum of All Servers'},
                   {type: 'avg', name: 'Average Across All Servers'},
                   {type: 'server', name: 'Specific Server'}];
  $scope.labelProperty = function(x) {
    return x.name + ' (' + x.units + (x.form == 'aggregate' ? '/second)' : ')');
  };
  $scope.labelServer = function(x) {
    return x.location + ' ' + x.id;
  };
  $scope.axes = [];

  // helper functions
  $scope.addSeries = function() {
    $scope.chart.fetch.data.push({props: ['msgs.req_get', 'msgs.req_atomic'],
                                  group: 'sum', label: 'Throughput'});
  };

  $scope.updateAxes = function() {
    axes = [];
    vAxes = [];
    series = [];
    for (i = 0; i < $scope.chart.fetch.data.length; ++i) {
      prop = ChartPropertiesService.lookupProperty($scope.chart.fetch.data[i].props[0]);
      if (prop == null) {
        continue;
      }
      j = 0;
      for (j = 0; j < axes.length; ++j) {
        if (axes[j].units == prop.units) {
          break;
        }
      }
      if (j == axes.length) {
        axes.push({units: prop.units});
        k = 0;
        for (k = 0; k < $scope.chart.options.vAxes.length; ++k) {
          if ($scope.chart.options.vAxes[k].units == prop.units) {
            break;
          }
        }
        if (k == $scope.chart.options.vAxes.length) {
          vAxes.push({units: prop.units, minValue: 0});
        } else {
          vAxes.push($scope.chart.options.vAxes[k]);
        }
      }
      series.push({targetAxisIndex: j});
    }
    $scope.axes = axes;
    $scope.chart.options.vAxes = vAxes;
    $scope.chart.options.series = series;
  };

  $scope.$watch('chart.fetch.data', $scope.updateAxes, true);
  $scope.$watch(function() {
    return {properties: ChartPropertiesService.properties};
  }, function() {
    $scope.properties = ChartPropertiesService.properties;
  }, true);
  $scope.$watch(function() {
    return {servers: ClusterConfigService.config.servers};
  }, function() {
    $scope.servers = ClusterConfigService.config.servers;
  }, true);
}]);

app.directive('chart', ['$http', 'BackendPropertyService', 'ChartRefreshService', 'GoogleVisualizationService',
        function ($http, BackendPropertyService, ChartRefreshService, GoogleVisualizationService) {
    return {
      replace: true,
      transclude: false,
      scope: {
        chart: '=source'
      },
  template: '<div><div id="chart{{$id}}"></div><div ng-show="!rendered"><progress style="margin: 0" percent="100" class="progress-info progress-striped active"></progress></div></div>',
      link: function(scope, element, attrs) {
        scope.dirty = true;
        scope.vis = null;
        scope.type = null;
        scope.data = null;
        scope.fetchCount = 0;
        scope.fetched = false;
        scope.rendered = false;

        scope.url = function() {
          return BackendPropertyService.url + '/chart.json?callback=JSON_CALLBACK&args=' + encodeURIComponent(angular.toJson(scope.chart.fetch));
        };

        scope.lookupType = function(t) {
          divid = 'chart' + scope.$id;
          if (t == 'bar') {
            return new google.visualization.BarChart(document.getElementById(divid));
          } else if (t == 'column') {
            return new google.visualization.ColumnChart(document.getElementById(divid));
          } else if (t == 'stepped') {
            return new google.visualization.SteppedAreaChart(document.getElementById(divid));
          } else if (t == 'line') {
            return new google.visualization.LineChart(document.getElementById(divid));
          } else {
            return null;
          }
        };

        scope.draw = function() {
          if (scope.fetched && scope.dirty && GoogleVisualizationService.setup()) {
            if (scope.vis == null || scope.type != scope.chart.type) {
              scope.vis = scope.lookupType(scope.chart.type);
              scope.type = scope.chart.type;
            }
            if (scope.vis != null) {
              scope.rendered = true;
              scope.vis.draw(scope.data, scope.chart.options);
            }
            scope.dirty = false;
          }
        };

        scope.fetch = function() {
          scope.fetchCount += 1;
          var x = scope.fetchCount;
          $http.jsonp(scope.url(), {timeout: 500})
            .success(function(data, status, headers, config) {
              if (x == scope.fetchCount) {
                table = google.visualization.arrayToDataTable(data);
                if (!angular.equals(table, scope.data)) {
                  scope.data = table;
                  scope.dirty = true;
                  scope.fetched = true;
                }
              }
            }).error(function(data, status, headers, config) {
              // XXX error
            });
        };

        scope.$watch(function() {
          return {url: BackendPropertyService.url,
                  fetch: scope.chart.fetch};
        }, function() {
          scope.fetch();
        }, true);

        scope.$watch(function() {
          return {chart: scope.chart};
        }, function() {
          scope.dirty = true;
        }, true);

        scope.$watch(function() {
          return {tick: ChartRefreshService.tick};
        }, function() {
          scope.draw();
          scope.fetch();
        }, true);
      }
    };
  }]);
