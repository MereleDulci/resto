/*
Package resto is an opinionated RESTful API framework for Go built to work with JSON:API and MongoDB.
It focuses on providing quick setup with minimal overhead and easy maintenance through strong conventions.


The main building blocks are:
	* ResourceHandle - a struct providing CRUD operations for a resource
	* ResourceEncoder - an interface you're required to implement to turn your struct into a compatible resource
	* Accessor - an intra-process interface to intract with resources bypassing HTTP
	* HTTP compatibility layer and JSON:API serialisation

This package uses zerolog for tracing. See zerolog documentation for more configuration options. By default the
global zerolog instance is used. You can overwrite it on ResourceHandle.
*/

package resto
