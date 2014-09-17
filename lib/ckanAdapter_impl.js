(function() {
  var http = require("http");
  var url = require("url");
  var _ckanInstance = "http://datahub.ubiapps.com/";
  var _ckanAPIKey = "0ccb9c39-ea12-40b0-a3d3-0d47da5f19c1";

  function CKANAdapterImpl(cfg) {
    this._config = cfg;
  }

  var ckanJSONRequest = function(endpoint, successCB, errorCB) {
    console.log("ckanJSONRequest: " + endpoint);
    var endpointOptions = url.parse(endpoint);
    endpointOptions.headers = {"Authorization": _ckanAPIKey};
    http.get(endpointOptions, function(res) {
      var chunks = [];
      res.on('data', function(chunk) {
        chunks.push(chunk);
      }).on('end', function() {
          var body = Buffer.concat(chunks).toString();
          try {
            var jsonBody = JSON.parse(body);
            if (jsonBody.success === true) {
              successCB(jsonBody.result);
            } else {
              errorCB(jsonBody.error);
            }
          } catch (e) {
            errorCB(e);
          }
        });
    }).on("error", errorCB);
  };

  CKANAdapterImpl.prototype.getFormat = function(successCB) {
    var self = this;
    process.nextTick(function() { successCB(self._config.format); });
  };

  CKANAdapterImpl.prototype.canBrowse = function(successCB) {
    var self = this;
    process.nextTick(function() { successCB(self._config.datastore); });
  };

  CKANAdapterImpl.prototype.getSchema = function(params, successCB, errorCB) {
    var schemaEndpoint = _ckanInstance + "api/3/action/datastore_search?limit=0&resource_id=" + this._config.resourceId;
    ckanJSONRequest(schemaEndpoint, function(resp) {
      successCB(resp.fields);
    }, errorCB);
  };

  var buildWhereClause = function(where, combine) {
    var clauseString = "";
    var combineTerm = combine === "all" ? " and " : " or ";

    var compareValue = function(clause) {
      var val;
      switch (clause.operator) {
        case "like":
          val = clause.literal.length > 0 ? "'%25" + clause.literal + "%25'" : "\"" + clause.field2 + "\"";
          break;
        default:
          val = clause.literal.length > 0 ? "'" + clause.literal + "'" : "\"" + clause.field2 + "\"";
          break;
      }
      return val;
    };

    where.forEach(function(clause) {
      if (clauseString.length > 0) clauseString += combineTerm;
      clauseString += "\"" + clause.field1 + "\" " + clause.operator + " " + compareValue(clause);
    });

    return clauseString.length > 0 ? " where " + clauseString : "";
  };

  CKANAdapterImpl.prototype.getRows = function(options, successCB, errorCB) {
    var searchEndpoint;
    if (options.hasOwnProperty("where")) {
      searchEndpoint = _ckanInstance + "api/3/action/datastore_search_sql?sql=select * from \"" + this._config.resourceId + "\"" + buildWhereClause(options.where, options.combine);
    } else {
      searchEndpoint = _ckanInstance + "api/3/action/datastore_search_sql?sql=select * from \"" + this._config.resourceId + "\"";
    }
    if (options.hasOwnProperty("sort")) {
      searchEndpoint += " order by \"" + options.sort + "\" " + options.sortDirection;
    }
    if (options.hasOwnProperty("pageSize") && options.hasOwnProperty("pageNum")) {
      searchEndpoint += " limit " + options.pageSize.toString() + " offset " + (options.pageSize*options.pageNum).toString();
    }
    ckanJSONRequest(searchEndpoint, function(resp) {
      successCB(resp.records);
    }, errorCB);
  };

  CKANAdapterImpl.prototype.getRowCount = function(options, successCB, errorCB) {
    var rowCountEndpoint;
    if (options.hasOwnProperty("where")) {
      rowCountEndpoint = _ckanInstance + "api/3/action/datastore_search_sql?sql=select count(*) from \"" + this._config.resourceId + "\"" + buildWhereClause(options.where, options.combine);
    } else {
      rowCountEndpoint = _ckanInstance + "api/3/action/datastore_search_sql?sql=select count(*) from \"" + this._config.resourceId + "\"";
    }
    ckanJSONRequest(rowCountEndpoint, function(resp) {
      successCB(resp.records);
    }, errorCB);
  };

  CKANAdapterImpl.prototype.getDownloadUrl = function(successCB, errorCB) {
    var resourceEndpoint = _ckanInstance + "api/3/action/resource_show?id=" + this._config.resourceId;
    ckanJSONRequest(resourceEndpoint, function(resp) {
      successCB(resp.url);
    }, errorCB);
  };

  module.exports = CKANAdapterImpl;
}());