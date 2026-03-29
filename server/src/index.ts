import { randomUUID } from 'crypto';
import { WebSocketServer, WebSocket, type WebSocket as ClientSocket } from 'ws';
import type {
  CreateGameData,
  Game,
  JoinGameData,
  Player,
  Question,
  RegData,
  WSMessage,
} from './types.js';

const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;

const usersByName = new Map<string, { password: string; index: string }>();
const userIndexToName = new Map<string, string>();
const socketToUserIndex = new Map<ClientSocket, string>();

const gamesById = new Map<string, Game>();
const gameIdByCode = new Map<string, string>();
const hostWaitingGameId = new Map<ClientSocket, string>();

const gameHostSockets = new Map<string, ClientSocket>();
const socketToGameId = new Map<ClientSocket, string>();

function sendMessage(ws: ClientSocket, type: string, data: unknown) {
  ws.send(JSON.stringify({ type, data, id: 0 }));
}

function safeSend(ws: ClientSocket, type: string, data: unknown) {
  if (ws.readyState === WebSocket.OPEN) {
    sendMessage(ws, type, data);
  }
}

function getPlayersPayload(game: Game) {
  return game.players.map((p) => ({
    name: p.name,
    index: p.index,
    score: p.score,
  }));
}

function broadcastToGame(gameId: string, type: string, data: unknown) {
  const game = gamesById.get(gameId);
  if (!game) return;
  const payload = JSON.stringify({ type, data, id: 0 });
  const hostWs = gameHostSockets.get(gameId);
  if (hostWs && hostWs.readyState === WebSocket.OPEN) {
    hostWs.send(payload);
  }
  for (const p of game.players) {
    if (p.ws && p.ws.readyState === WebSocket.OPEN) {
      p.ws.send(payload);
    }
  }
}

function handleReg(ws: ClientSocket, data: unknown) {
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
    userIndexToName.set(existing.index, name);
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
  userIndexToName.set(index, name);
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

function removeWaitingGameForHost(ws: ClientSocket) {
  const prevId = hostWaitingGameId.get(ws);
  if (!prevId) return;
  hostWaitingGameId.delete(ws);
  const prev = gamesById.get(prevId);
  if (prev && prev.status === 'waiting') {
    for (const p of prev.players) {
      if (p.ws) {
        safeSend(p.ws, 'error', { message: 'Game was cancelled' });
        socketToGameId.delete(p.ws);
      }
    }
    gamesById.delete(prevId);
    gameIdByCode.delete(prev.code);
    gameHostSockets.delete(prevId);
  }
}

function handleCreateGame(ws: ClientSocket, data: unknown) {
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
  gameHostSockets.set(gameId, ws);

  sendMessage(ws, 'game_created', { gameId, code });
}

function handleJoinGame(ws: ClientSocket, data: unknown) {
  const userId = socketToUserIndex.get(ws);
  if (!userId) {
    sendMessage(ws, 'error', { message: 'Register before joining a game' });
    return;
  }

  const raw = data as JoinGameData;
  const code =
    typeof raw?.code === 'string' ? raw.code.trim().toUpperCase() : '';
  if (code.length !== 6) {
    sendMessage(ws, 'error', { message: 'Invalid room code' });
    return;
  }

  const gameId = gameIdByCode.get(code);
  if (!gameId) {
    sendMessage(ws, 'error', { message: 'Game not found' });
    return;
  }

  const game = gamesById.get(gameId);
  if (!game || game.status !== 'waiting') {
    sendMessage(ws, 'error', { message: 'Game is not available to join' });
    return;
  }

  if (game.hostId === userId) {
    sendMessage(ws, 'error', { message: 'Host cannot join as a player' });
    return;
  }

  if (game.players.some((p) => p.index === userId)) {
    sendMessage(ws, 'error', { message: 'Already in this game' });
    return;
  }

  const displayName = userIndexToName.get(userId);
  if (!displayName) {
    sendMessage(ws, 'error', { message: 'User profile not found' });
    return;
  }

  const player: Player = {
    name: displayName,
    index: userId,
    score: 0,
    ws,
  };
  game.players.push(player);
  socketToGameId.set(ws, gameId);

  sendMessage(ws, 'game_joined', { gameId });

  const playerCount = game.players.length;
  broadcastToGame(gameId, 'player_joined', {
    playerName: displayName,
    playerCount,
  });

  broadcastToGame(gameId, 'update_players', getPlayersPayload(game));
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
      case 'join_game':
        handleJoinGame(ws, msg.data);
        break;
      default:
        break;
    }
  });

  ws.on('close', () => {
    const joinedGameId = socketToGameId.get(ws);
    if (joinedGameId) {
      const game = gamesById.get(joinedGameId);
      if (game) {
        game.players = game.players.filter((p) => p.ws !== ws);
        socketToGameId.delete(ws);
        broadcastToGame(joinedGameId, 'update_players', getPlayersPayload(game));
      }
    }
    socketToUserIndex.delete(ws);
    removeWaitingGameForHost(ws);
  });
});

console.log(`WebSocket server listening on ws://localhost:${PORT}`);
