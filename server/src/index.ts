import { randomUUID } from 'crypto';
import { WebSocketServer, type WebSocket } from 'ws';
import type { RegData, WSMessage } from './types.js';

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;

const usersByName = new Map<string, { password: string; index: string }>();

const socketToUserIndex = new Map<WebSocket, string>();

function sendMessage(ws: WebSocket, type: string, data: unknown) {
  ws.send(JSON.stringify({ type, data, id: 0 }));
}

function handleReg(ws: WebSocket, data: unknown) {
  const raw = data as RegData;
  const name = typeof raw?.name === 'string' ? raw.name.trim() : '';
  const password = typeof raw?.password === 'string' ? raw.password : '';

  if (!name || !password) {
    sendMessage(ws, 'reg', {
      name: name || '',
      index: '',
      error: true,
      errorText: 'Name and password are required',
    });
    return;
  }

  const existing = usersByName.get(name);

  if (existing) {
    if (existing.password !== password) {
      sendMessage(ws, 'reg', {
        name,
        index: '',
        error: true,
        errorText: 'Invalid password',
      });
      return;
    }

    socketToUserIndex.set(ws, existing.index);
    sendMessage(ws, 'reg', {
      name,
      index: existing.index,
      error: false,
      errorText: '',
    });
    return;
  }

  const index = randomUUID();
  usersByName.set(name, { password, index });
  socketToUserIndex.set(ws, index);

  sendMessage(ws, 'reg', {
    name,
    index,
    error: false,
    errorText: '',
  });
}

const wss = new WebSocketServer({ port: PORT });

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let msg: WSMessage;
    try {
      msg = JSON.parse(raw.toString()) as WSMessage;
    } catch {
      sendMessage(ws, 'error', { message: 'Invalid JSON' });
      return;
    }

    switch (msg.type) {
      case 'reg':
        handleReg(ws, msg.data);
        break;
      default:
        break;
    }
  });

  ws.on('close', () => {
    socketToUserIndex.delete(ws);
  });
});

console.log(`WebSocket server listening on ws://localhost:${PORT}`);
