import { randomUUID } from 'crypto';
import { WebSocketServer, type WebSocket } from 'ws';
import type { CreateGameData, Game, Question, RegData, WSMessage } from './types.js';

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;

const usersByName = new Map<string, { password: string; index: string }>();
const socketToUserIndex = new Map<WebSocket, string>();

const gamesById = new Map<string, Game>();
const gameIdByCode = new Map<string, string>();
const hostWaitingGameId = new Map<WebSocket, string>();

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

const CODE_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';

function generateRoomCode(): string {
  let code = '';
  for (let i = 0; i < 6; i++) {
    code += CODE_CHARS[Math.floor(Math.random() * CODE_CHARS.length)];
  }
  return code;
}

function generateUniqueRoomCode(): string {
  let code = generateRoomCode();
  while (gameIdByCode.has(code)) {
    code = generateRoomCode();
  }
  return code;
}

function validateQuestions(questions: unknown): questions is Question[] {
  if (!Array.isArray(questions) || questions.length === 0) return false;
  for (const q of questions) {
    if (!q || typeof q !== 'object') return false;
    const obj = q as Question;
    if (typeof obj.text !== 'string' || !obj.text.trim()) return false;
    if (!Array.isArray(obj.options) || obj.options.length !== 4) return false;
    if (!obj.options.every((o) => typeof o === 'string')) return false;
    const ci = obj.correctIndex;
    if (typeof ci !== 'number' || !Number.isInteger(ci) || ci < 0 || ci > 3) return false;
    const tl = obj.timeLimitSec;
    if (typeof tl !== 'number' || !Number.isFinite(tl) || tl <= 0) return false;
  }
  return true;
}

function removeWaitingGameForHost(ws: WebSocket) {
  const prevId = hostWaitingGameId.get(ws);
  if (!prevId) return;
  const prev = gamesById.get(prevId);
  if (prev && prev.status === 'waiting') {
    gamesById.delete(prevId);
    gameIdByCode.delete(prev.code);
  }
  hostWaitingGameId.delete(ws);
}

function handleCreateGame(ws: WebSocket, data: unknown) {
  const hostId = socketToUserIndex.get(ws);
  if (!hostId) {
    sendMessage(ws, 'error', { message: 'Register before creating a game' });
    return;
  }

  const raw = data as CreateGameData;
  if (!validateQuestions(raw?.questions)) {
    sendMessage(ws, 'error', {
      message:
        'Invalid questions: need at least one question with text, exactly 4 options, correctIndex 0–3, and timeLimitSec > 0',
    });
    return;
  }

  removeWaitingGameForHost(ws);

  const gameId = randomUUID();
  const code = generateUniqueRoomCode();
  gameIdByCode.set(code, gameId);

  const game: Game = {
    id: gameId,
    code,
    hostId,
    questions: raw.questions.map((q) => ({
      text: q.text.trim(),
      options: q.options.map((o) => o),
      correctIndex: q.correctIndex,
      timeLimitSec: q.timeLimitSec,
    })),
    players: [],
    currentQuestion: -1,
    status: 'waiting',
    playerAnswers: new Map(),
  };

  gamesById.set(gameId, game);
  hostWaitingGameId.set(ws, gameId);

  sendMessage(ws, 'game_created', { gameId, code });
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
      case 'create_game':
        handleCreateGame(ws, msg.data);
        break;
      default:
        break;
    }
  });

  ws.on('close', () => {
    socketToUserIndex.delete(ws);
    removeWaitingGameForHost(ws);
  });
});

console.log(`WebSocket server listening on ws://localhost:${PORT}`);
