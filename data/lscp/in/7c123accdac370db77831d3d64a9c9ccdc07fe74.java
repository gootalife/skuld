hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/CapacitySchedulerPage.java
          "  $('#cs').bind('select_node.jstree', function(e, data) {",
          "    var q = $('.q', data.rslt.obj).first().text();",
          "    if (q == 'Queue: root') q = '';",
          "    else {",
          "      q = q.substr(q.lastIndexOf(':') + 2);",
          "      q = '^' + q.substr(q.lastIndexOf('.') + 1) + '$';",
          "    }",
          "    $('#apps').dataTable().fnFilter(q, 4, true);",
          "  });",
          "  $('#cs').show();",

