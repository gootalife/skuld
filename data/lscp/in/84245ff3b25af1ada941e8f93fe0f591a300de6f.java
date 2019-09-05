hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/java/org/apache/hadoop/yarn/webapp/view/JQueryUI.java
    String defaultInit = "{bJQueryUI: true, sPaginationType: 'full_numbers'}";
    String stateSaveInit = "bStateSave : true, " +
        "\"fnStateSave\": function (oSettings, oData) { " +
              " data = oData.aoSearchCols;"
              + "for(i =0 ; i < data.length; i ++) {"
              + "data[i].sSearch = \"\""
              + "}"
        + " sessionStorage.setItem( oSettings.sTableId, JSON.stringify(oData) ); }, " +
          "\"fnStateLoad\": function (oSettings) { " +
              "return JSON.parse( sessionStorage.getItem(oSettings.sTableId) );}, ";
      

