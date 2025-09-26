const WebSocket = require('ws');

const wss = new WebSocket.Server({ port: 8765 });

wss.on('connection', (ws) => {
  console.log('New WebSocket connection established.');

  ws.on('error', (error) => {
    console.error('WebSocket error:', error);
  });

  sendMessageAndClose(ws);

});

async function sendMessageAndClose(ws) {
  for(var i = 1; i < 10; i++) {
    var message = {id: i, name: `film_${i}`, year: 1950 + i};
    console.log('Sending film_' + i);
    ws.send(JSON.stringify(message));
    await new Promise(r => setTimeout(r, 2000));
  }

  ws.close();
}

console.log('WebSocket server is running at ws://localhost:8765');