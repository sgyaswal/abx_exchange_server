const net = require('net');
const packetData = {
  packetStream: [
    { symbol: 'AAPL', buysellindicator: 'B', quantity: 50, price: 100, packetSequence: 1 },
    { symbol: 'AAPL', buysellindicator: 'B', quantity: 30, price: 98, packetSequence: 2 },
    { symbol: 'AAPL', buysellindicator: 'S', quantity: 20, price: 101, packetSequence: 3 },
    { symbol: 'AAPL', buysellindicator: 'S', quantity: 10, price: 102, packetSequence: 4 },
    { symbol: 'MSFT', buysellindicator: 'B', quantity: 40, price: 50, packetSequence: 5 },
    { symbol: 'MSFT', buysellindicator: 'S', quantity: 30, price: 55, packetSequence: 6 },
    { symbol: 'MSFT', buysellindicator: 'S', quantity: 20, price: 57, packetSequence: 7 },
    { symbol: 'AMZN', buysellindicator: 'B', quantity: 25, price: 150, packetSequence: 8 },
    { symbol: 'AMZN', buysellindicator: 'S', quantity: 15, price: 155, packetSequence: 9 },
    { symbol: 'AMZN', buysellindicator: 'B', quantity: 20, price: 148, packetSequence: 10 },
    { symbol: 'META', buysellindicator: 'B', quantity: 10, price: 3000, packetSequence: 11 },
    { symbol: 'META', buysellindicator: 'B', quantity: 5, price: 2999, packetSequence: 12 },
    { symbol: 'META', buysellindicator: 'S', quantity: 15, price: 3020, packetSequence: 13 },
    { symbol: 'AMZN', buysellindicator: 'S', quantity: 10, price: 3015, packetSequence: 14 },
  ]
};

const PACKET_CONTENTS = [
  { name: 'symbol', type: 'ascii', size: 4 },
  { name: 'buysellindicator', type: 'ascii', size: 1 },
  { name: 'quantity', type: 'int32', size: 4 },
  { name: 'price', type: 'int32', size: 4 },
  { name: 'packetSequence', type: 'int32', size: 4 }
];

const PACKET_SIZE = PACKET_CONTENTS.reduce((sum, field) => sum + field.size, 0);

function createPayloadToSend(packet) {
  let offset = 0;
  const buffer = Buffer.alloc(PACKET_SIZE);

  PACKET_CONTENTS.forEach(field => {
    const { name, type, size } = field;

    if (type === 'int32') {
      offset = buffer.writeInt32BE(packet[name], offset);
    } else {
      offset += buffer.write(packet[name], offset, size, 'ascii');
    }
  });

  return buffer;
}

let BUFFER_COLLECTOR = Buffer.alloc(0);
const orderBook = packetData.packetStream;

const server = net.createServer(socket => {
  console.log('Client connected.');

  let clientDisconnected = false; // Track the client disconnection status

  socket.on('data', data => {
    BUFFER_COLLECTOR = Buffer.concat([BUFFER_COLLECTOR, data]);

    while (BUFFER_COLLECTOR.length >= 2) {
      const header = BUFFER_COLLECTOR.slice(0, 2);
      const packetType = header.readInt8(0);
      const packetSequence = header.readInt8(1);

      BUFFER_COLLECTOR = BUFFER_COLLECTOR.slice(2);

      if (packetType === 1) {
        orderBook.forEach(packet => {
          if (Math.random() > 0.75) return; // Simulate some packets not being sent
          const payload = createPayloadToSend(packet);

          if (!socket.destroyed && !clientDisconnected) { // Ensure socket is not closed
            socket.write(payload);
          }
        });

        // Delay closing to ensure all packets are sent
        setTimeout(() => {
          if (!socket.destroyed && !clientDisconnected) {
            socket.end();
            clientDisconnected = true; // Mark client as disconnected
          }
        }, 1000); // Small delay to let packets be written
        console.log('Packets sent. Client disconnected.');
      } else if (packetType === 2 && !clientDisconnected) { // Handle resending packet
        const resendPacket = orderBook.find(packet => packet.packetSequence === packetSequence);
        const payload = createPayloadToSend(resendPacket);

        if (!socket.destroyed) { // Ensure socket is not closed
          socket.write(payload);
          console.log('Packet resent.');
        }
      }
    }
  });

  socket.on('end', () => {
    console.log('Client disconnected.');
    clientDisconnected = true; // Set flag to prevent further writes
  });

  socket.on('error', err => {
    console.error('Socket error:', err);
    clientDisconnected = true; // Set flag to prevent further writes
  });
});

server.listen(3000, () => {
  console.log('TCP server started on port 3000.');
});
