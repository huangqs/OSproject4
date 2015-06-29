var http = require('http');
var url = require('url');
var querystring = require('querystring');
var fs = require('fs');

var conf = JSON.parse(fs.readFileSync('conf/settings.conf'));
conf.ips = [];
conf.comm_ports = [];
var m = 0;
while (m < 100) {
	var t = conf['n' + ('0' + (m+1).toString()).substr(-2)];
	if(t == undefined) break;
    var tt = t.split(':');
	conf.ips[m] = tt[0];
	conf.comm_ports[m++] = tt[1];
}
if (conf.port != undefined) {
	conf.ports = [];
	for (var i = 0; i < m; i++) conf.ports[i] = conf.port;
}
var nodeID = parseInt(process.argv[2].substr(-2)) - 1;

var dict = {};
var paxosQueue = [];
var min = 0;
var max = 0;
var cur = [];
for (var i = 0; i < m; i++) cur[i] = 0;

function onUpdateCur() {
	max = Math.max.apply(Math, cur);
	var x = Math.min.apply(Math, cur);
	while (min < x) {
		paxosQueue.shift();
		min++;
	}
}

var queue = [];

function jsonRespond(response, msg) {
	response.writeHead(200, {'Content-Type': 'application/json'});
	response.end(JSON.stringify(msg));
}

function startServer(port, handlers) {
	http.createServer(function(request,response) {
		var req_url = url.parse(request.url,true);
		var handler = handlers[req_url.pathname];
		if (handler == undefined) {
			response.writeHead(404, {'Content-Type': 'text/plain'});
			response.end('Not found: ' + req_url.pathname);
		}
		else {
			if (request.method == 'GET') handler(req_url.query, response);
			else if(request.method == 'POST') {
				var postData = '';
				request.addListener('data', function(postDataChunk) {
					postData += postDataChunk;
				});
				request.addListener('end', function() {
					query = querystring.parse(postData);
					handler(query, response);
				});
			}
			else {
				response.writeHead(200, {'Content-Type': 'text/plain'});
				response.end('Unknown method: ' + request.method);
			}
		}
	}).listen(port);
}

function broadcast(op, query, callback) {
	process.nextTick(function(){callback(paxos_handlers[op](query));});
	for (var i = 0; i < m; i++) if (i != nodeID) {
		str = querystring.stringify({'query': JSON.stringify(query)});
		var request = http.request({host: conf.ips[i], port: conf.comm_ports[i], path: op, method: 'POST',
			headers: {'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': str.length}},
			function(res) {
				var str = '';
				res.on('data', function(chunk){str += chunk;});
				res.on('end', function() {
                    var value = undefined;
                    try {value = JSON.parse(str);} catch(e) {}
					callback(value);
				});
			});
		//request.on('error', function(){var req = request; return function(){req.abort(); callback();};}());
		request.on('error', function(){callback();});
		request.write(str);
		request.end();
	}
}

function getPaxos(seq) {
	var t = seq - min;
	while (paxosQueue.length <= t) paxosQueue.push({'n_p': -1, 'n_a': -1});
	return paxosQueue[t];
}

function paxosPropose(seq, v, callback, delay) {
    setTimeout(function() {
        var paxos = getPaxos(seq);
        var n = paxos.n_p + 1;
        while (n % m != nodeID) n++; // todo: not elegant
        broadcast('/paxos/prepare', {'seq': seq, 'n': n}, function() {
            var nResponse = 0, nError = 0, n_ = -1, v_ = v, alive = true;
            return function(response) {
                if (!alive) return;
                if (response == undefined) {
                    nError++;
                    if (nError * 2 >= m) {
                        alive = false;
                        paxosPropose(seq, v, callback, delay);
                    }
                }
                else if (!response.success) {
                    paxos.n_p = response.n;
                    alive = false;
                    paxosPropose(seq, v, callback, delay);
                }
                else {
                    nResponse++;
                    if (response.success) {
                        if (response.n > n_) {
                            n_ = response.n;
                            v_ = response.v;
                        }
                        if (nResponse * 2 > m) {
                            alive = false;
                            broadcast('/paxos/accept', {'seq': seq, 'n': n, 'v': v_}, function() {
                                var nResponse = 0, nError = 0, alive = true;
                                return function(response) {
                                    if (!alive) return;
                                    if (response == undefined) {
                                        nError++;
                                        if (nError * 2 >= m) {
                                            alive = false;
                                            paxosPropose(seq, v, callback, delay);
                                        }
                                    }
                                    else if (!response.success) {
                                        paxos.n_p = response.n;
                                        alive = false;
                                        paxosPropose(seq, v, callback, delay);
                                    }
                                    else {
                                        nResponse ++;
                                        if (nResponse * 2 > m) {
                                            alive = false;
                                            console.log('decided ' + JSON.stringify(v_) + ' for ' + seq);
                                            callback(v_);
                                        }
                                    }
                                };
                            }());
                        }
                    }
                }
            };
        }());
    }, delay);
    if(delay < 1000) delay = delay * 2 + 1;
    else delay = Math.floor(Math.random() * 1000 + 1000);
}

var client_handlers = {
	'/kvman/countkey': function(query, response) {
		jsonRespond(response, {'result': Object.keys(dict).length});
	},
	'/kvman/dump': function(query, response) {
		jsonRespond(response, dict);
	},
	'/kvman/shutdown': function(query, response) {
		response.writeHead(200, {'Content-Type': 'text/plain'});
		response.end('');
		process.exit();
	},
	'/kv/': function(query, response) {
		response.writeHead(200, {'Content-Type':'text/html'});
		response.end(fs.readFileSync('index.htm'));
	}
};

var paxos_handlers = {
	'/paxos/prepare': function(query) {
		console.log('prepare ' + JSON.stringify(query));
		var paxos = getPaxos(query.seq);
		if (paxos == undefined) return; // ancient message
		if (query.n <= paxos.n_p) return {'success': false, 'n': paxos.n_p};
		paxos.n_p = query.n;
		return {'success': true, 'n': paxos.n_a, 'v': paxos.v_a};	
	},
	'/paxos/accept': function(query) {
		console.log('accept ' + JSON.stringify(query));
		var paxos = getPaxos(query.seq);
		if (paxos == undefined) return; // ancient message
		if (query.n < paxos.n_p) return {'success': false, 'n': paxos.n_p};
		paxos.n_p = query.n;
		paxos.n_a = query.n;
		paxos.v_a = query.v;
		return {'success': true};
	},
	'/paxos/sync': function(query) {
		//console.log('sync ' + JSON.stringify(query));
		for (var i = 0; i < m; i++) {
			if (cur[i] < query.cur[i]) cur[i] = query.cur[i];
		}
		onUpdateCur();
		return {};
	}
};

var kv_handlers = {
	'/kv/insert': function(query) {
		if (query.key == undefined || query.value == undefined || dict[query.key] != undefined)
			return {'success': false};
		dict[query.key] = query.value;
		return {'success': true};
	},
	'/kv/delete': function(query) {
		var value = dict[query.key];
		if(value == undefined) return {'success': false};
		delete dict[query.key];
		return {'success': true, 'value': value};
	},
	'/kv/get': function(query) {
		var value = dict[query.key];
		return {'success': value != undefined, 'value': value};
	},
	'/kv/update': function(query) {
		if (query.value == undefined || dict[query.key] == undefined) return {'success': false};
		dict[query.key] = query.value;
		return {'success': true};
	}
};

var comm_handlers = {
	'/paxos/test': function(query, response) {
		jsonRespond(response, {'min': min, 'cur': cur, 'queue': paxosQueue});
	}
};

for (var key in paxos_handlers)
	comm_handlers[key] = function() {
		var handler = paxos_handlers[key];
		return function(query, response) {
			jsonRespond(response, handler(JSON.parse(query.query)));
		};
	}();

function paxosTry() {
	paxosPropose(cur[nodeID], queue[0].request, function(value) {
		cur[nodeID]++;
		onUpdateCur();
		var res = kv_handlers[value.op](value.query);
		if (queue[0].response == undefined) {if(cur[nodeID] == max) queue.shift();}
		else if (value.me == nodeID) jsonRespond(queue.shift().response, res);
		if (queue.length > 0) paxosTry();
	}, 0);
}

for (var key in kv_handlers)
	client_handlers[key] = function() {
		var op = key;
		return function(query, response) {
			queue.push({'response': response, 'request': {'op': op, 'query': query, 'me': nodeID}});
			if (queue.length == 1) paxosTry();
		};
	}();

function sync() {
	broadcast('/paxos/sync', {'cur': cur}, function(){});
	if (cur[nodeID] < max && queue.length == 0) {
		queue.push({'request': {}});
		paxosTry();
	}
	setTimeout(sync, 1000);
}

startServer(conf.comm_ports[nodeID], comm_handlers);
startServer(conf.ports[nodeID], client_handlers);
sync();
