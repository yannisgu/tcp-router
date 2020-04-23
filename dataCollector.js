#!/usr/bin/env node
const net = require("net");
const amqp = require("amqplib");

function startServer(sendData) {
  const defaultPort = 12345;
  const defaultHost = "0.0.0.0";
  const server = net.createServer();
  server.listen(defaultPort, defaultHost, () => {
    console.log("TCP Server is running on port " + defaultPort + ".");
  });

  server.on("connection", function(sock) {
    console.log("CONNECTED: " + sock.remoteAddress + ":" + sock.remotePort);

    sock.on("data", function(data) {
      console.log("got data", data);
      sendData(data);
    });

    // Add a 'close' event handler to this instance of socket
    sock.on("close", function(data) {
      console.log("CLOSED: " + sock.remoteAddress + " " + sock.remotePort);
    });
  });
}

const retrySeconds = 5;

function openQueue(config) {
  async function connect() {
    try {
      const conn = await amqp.connect(config.url);
      conn.on("close", e => {
        console.log(
          `Connection to server ${config.url} closed, reconnect in ${retrySeconds} seconds`,
          e.message
        );
        setTimeout(connect, retrySeconds * 1000);
      });
      conn.on("error", e => {
        console.log(
          `Connection to server ${config.url} closed, reconnect in ${retrySeconds} seconds`,
          e.message
        );
        setTimeout(connect, retrySeconds * 1000);
      });
      channel = await conn.createChannel();
      await channel.assertQueue(config.queue, { durable: true });
    } catch (e) {
      console.log(
        `Could not connect to server ${config.url}, reconnect in ${retrySeconds} seconds`,
        e.message
      );
      setTimeout(connect, retrySeconds * 1000);
    }
  }

  connect();

  return async function(data) {
    await channel.sendToQueue(config.queue, data);
  };
}

const sendData = openQueue({ url: "amqp://localhost", queue: "data" });
startServer(sendData);
