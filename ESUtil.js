/**
 * This util provides helper functions to create a request for ES server
 */

var elasticsearch = require('elasticsearch'),
    logger = require('winston'),
    util = require('util'),
    client,
    Query;



//Returns ES client that enables communication between client and server
exports.getClient = function(){
  return client
}

//Returns an empty Query object. This object have utility methods to specify various ES query params
exports.createQueryObject = function(){
  return new Query()
}

//create an ES client to communicate with ES server
client = new elasticsearch.Client({
  host: 'localhost:9201',
  log: 'trace'
});

Query = function(){
  this.query = {

  }
}
Query.prototype.setRange = function(){

}

/**
 * Method to limit the number fields returned in result
 * @param {Array} fields - An array of fields required in output
 */
Query.prototype.setFields = function(fields){
  if(util.isArray(fields)){
    this.fields = fields
  }
}

Query.prototype.createSortField = function(){
    this.query.sort = []
}

Query.prototype.addSortRule = function(rule){
  if(util.isArray(rule)){
    this.query["sort"] = rule
  } else {
    if(util.isArray(this.query.sort)){
      this.query.sort.push(rule)
    } else {
      this.createSortField();
    }
  }
}


Query.prototype.createMatchRule = function(fields,query){
  var obj = {match:{}}
  obj.match[fields] = query

  return obj
}

Query.prototype.createQueryStringRule = function(fields,query){
  var obj = {query_string:{}}

  obj["query_string"].fields = []

  if(typeof fields === "string"){
    obj["query_string"].fields.push(fields)
  } else if(util.isArray(fields)){
    obj["query_string"].fields = fields
  }

  obj["query_string"].query = query

  return obj
}
Query.prototype.createNestedRule = function(path,fields,query){
  return {
    "nested": {
      "query": {
        "query_string": {
          "fields": [fields],
          "query": query/*,
          "fuzzy_max_expansions" : 0,
           "fuzziness" : 0*/
        },
      path : path
      }
    }
  }
}

/**
 * Adds empty bool query object to body of query
 */
Query.prototype.addBoolFragment = function(){
  if(!this.query.hasOwnProperty('bool')){
    this.query['bool'] = {}
  }
}

/**
 * Takes an array as input and adds must query object to bool query object
 * @param mustFragment
 */
Query.prototype.addMustToBoolQuery = function(mustFragment){
  if(!this.query.hasOwnProperty('bool')){
    this.addBoolFragment()
  }
  if(!this.query.bool.hasOwnProperty('must')){
    this.query.bool['must'] = [];
  }
  this.query.bool['must'].push(mustFragment)
}

/**
 * Takes an array as input and adds should query object to bool query object
 * @param shouldFragment
 */
Query.prototype.addShouldToBoolQuery = function(shouldFragment){
  if(!this.query.hasOwnProperty('bool')){
    this.addBoolFragment()
  }
  this.query.bool['should'] = shouldFragment
}

/**
 *
 * @param searchKey - Field on which range is to applied
 * @param from - Starting value for range
 * @param to - Upper limit for range
 * @returns Object for range query
 */
Query.prototype.createRange = function(searchKey, from, to){
  var output = {};
  output['range'] = {};

  output.range[searchKey] = {
    "from" : from,
    "to" : to
  }
  return output
}

/**
 *
 * @param termKey - Field to search in
 * @param termValue - Term to search in given field
 * @returns Object for Term based query
 */
Query.prototype.createTerm = function(termKey, termValue){
  var obj = {term:{}}
  obj.term[termKey] = termValue
  return obj
}

Query.prototype.createMust = function(mustQuery){
  return {
    "must" : mustQuery
  }
}

