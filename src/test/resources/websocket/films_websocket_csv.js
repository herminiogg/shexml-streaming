const WebSocket = require('ws');

const wss = new WebSocket.Server({ host: "0.0.0.0", port: 8765 });

wss.on('connection', (ws) => {
  console.log('New WebSocket connection established.');

  ws.on('error', (error) => {
    console.error('WebSocket error:', error);
  });

  sendMessageAndClose(ws);

});

async function sendMessageAndClose(ws) {
  for(var i = 1; i < 10; i++) {
    var message = "id;name;year;data\n" +
                  `${i};film_${i};${1950 + i}`
    console.log('Sending film_' + i);
    ws.send(message);
    await new Promise(r => setTimeout(r, 2000));
  }

  ws.close();
}

console.log('WebSocket server is running at ws://localhost:8765');