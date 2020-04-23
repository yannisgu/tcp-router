#!/usr/bin/env node
const net = require("net");
const amqp = require("amqplib");

function startServer({ start, stop }) {
  const defaultPort = 23456;
  const defaultHost = "0.0.0.0";
  const server = net.createServer();
  let sockets = [];
  server.listen(defaultPort, defaultHost, () => {
    console.log("TCP Server is running on port " + defaultPort + ".");
  });

  let started = false;

  server.on("connection", function(sock) {
    console.log("OPEN: " + sock.remoteAddress + " " + sock.remotePort);
    sockets.push(sock);
    if (!started) {
      started = true;
      start(sendData);
    }

    // Add a 'close' event handler to this instance of socket
    sock.on("close", function(data) {
      console.log("CLOSED: " + sock.remoteAddress + " " + sock.remotePort);
      sockets = sockets.filter(_ => _ !== sock);
      if (sockets.length === 0) {
        started = false;
        stop();
      }
    });
  });

  function sendData(data) {
    if (sockets.length > 0) {
      for (const socket of sockets) {
        socket.write(data);
      }
      return true;
    } else {
      return false;
    }
  }
}

const retrySeconds = 5;

function openQueue(config, callback) {
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
      channel.consume(config.queue, message => {
        try {
          console.log("got data", message.content);
          const result = callback(message.content);
          console.log("result", result);
          if (result) {
            channel.ack(message);
          }
        } catch {
          console.log("error");
        }
      });
    } catch (e) {
      console.log(
        `Could not connect to server ${config.url}, reconnect in ${retrySeconds} seconds`,
        e.message
      );
      setTimeout(connect, retrySeconds * 1000);
    }
  }

  connect();

  return {
    stop: () => {
      channel.cancel("source");
    }
  };
}

let stopQueueFn;

const startQueue = sendData => {
  const { stop } = openQueue(
    { url: "amqp://localhost", queue: "data" },
    sendData
  );
  stopQueueFn = stop;
};
const stopQueue = () => {
  stopQueueFn();
};

startServer({ start: startQueue, stop: stopQueue });
