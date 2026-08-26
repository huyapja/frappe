const { Server } = require("socket.io");
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");

const { get_conf, get_redis_subscriber } = require("../node_utils");
const conf = get_conf();

const server = http.createServer();

let io = new Server(server, {
	cors: {
		// Should be fine since we are ensuring whether hostname and origin are same before adding setting listeners for s socket
		origin: true,
		credentials: true,
	},
	cleanupEmptyChildNamespaces: true,
});

// Multitenancy implementation.
// allow arbitrary sitename as namespaces
// namespaces get validated during authentication.
const realtime = io.of(/^\/.*$/);

// load and register middlewares
const authenticate = require("./middlewares/authenticate");
realtime.use(authenticate);
// =======================

// load and register handlers
const frappe_handlers = require("./handlers/frappe_handlers");
const _app_handlers = {};

function get_app_handlers(app) {
	if (app in _app_handlers) {
		return _app_handlers[app];
	}

	const file = `../../${app}/realtime/handlers.js`;
	const abs_path = path.resolve(__dirname, file);
	let handler = null;

	if (fs.existsSync(abs_path)) {
		try {
			handler = require(file);
		} catch (err) {
			console.warn(`Failed to load realtime handlers from ${abs_path}`);
			console.warn(err);
		}
	}

	_app_handlers[app] = handler;
	return handler;
}

function on_connection(socket) {
	frappe_handlers(realtime, socket);

	// Backport custom app realtime handlers for Frappe v15.
	(socket.installed_apps || []).forEach((app) => {
		if (app === "frappe") return;

		const app_handler = get_app_handlers(app);
		if (!app_handler) return;

		try {
			app_handler(socket);
		} catch (err) {
			console.warn(`Failed to setup realtime handlers from ${app}`);
			console.warn(err);
		}
	});

	// ESBuild "open in editor" on error
	socket.on("open_in_editor", async (data) => {
		await subscriber.connect();
		subscriber.publish("open_in_editor", JSON.stringify(data));
	});
}

realtime.on("connection", on_connection);
// =======================

// Consume events sent from python via redis pub-sub channel.
const subscriber = get_redis_subscriber();

(async () => {
	await subscriber.connect();
	subscriber.subscribe("events", (message) => {
		message = JSON.parse(message);
		let namespace = "/" + message.namespace;
		if (message.room) {
			io.of(namespace).to(message.room).emit(message.event, message.message);
		} else {
			// publish to ALL sites only used for things like build event.
			realtime.emit(message.event, message.message);
		}
	});
})();
// =======================

let uds = conf.socketio_uds;
let port = conf.socketio_port;
server.listen(uds || port, () => {
	console.log("Realtime service listening on: ", uds || port);
});
