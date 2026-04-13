// ============================================================
// 一起看 - Cloudflare Worker WebSocket 中继服务
// 使用 Durable Objects 实现房间隔离
// ============================================================

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders() });
    }

    // Health check
    if (url.pathname === '/') {
      return new Response('yiqikan relay ok', { headers: corsHeaders() });
    }

    // WebSocket: /ws/:roomId
    const match = url.pathname.match(/^\/ws\/([A-Za-z0-9]{4,20})$/);
    if (match) {
      const roomId = match[1].toUpperCase();

      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Expected WebSocket', { status: 426, headers: corsHeaders() });
      }

      // Get or create Durable Object for this room
      const id = env.ROOM.idFromName(roomId);
      const room = env.ROOM.get(id);
      return room.fetch(request);
    }

    return new Response('Not Found', { status: 404, headers: corsHeaders() });
  }
};

function corsHeaders() {
  return {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
    'Access-Control-Allow-Headers': '*',
  };
}

// ============================================================
// Durable Object: Room
// ============================================================
export class Room {
  constructor(state, env) {
    this.state = state;
    this.sessions = new Map(); // ws -> { id, nickname }
    this.hostId = null;
    this.currentBvid = '';
    this.counter = 0;
  }

  async fetch(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.handleSession(server);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  handleSession(ws) {
    ws.accept();

    const sessionId = `u${++this.counter}`;
    this.sessions.set(ws, { id: sessionId, nickname: '未知' });

    // If this is the first user, they become host
    if (this.sessions.size === 1) {
      this.hostId = sessionId;
    }

    ws.addEventListener('message', (event) => {
      try {
        const data = JSON.parse(event.data);
        this.handleMessage(ws, data);
      } catch(e) {
        // ignore invalid JSON
      }
    });

    ws.addEventListener('close', () => {
      const session = this.sessions.get(ws);
      this.sessions.delete(ws);

      if (session) {
        // Notify others
        this.broadcastExcept(ws, {
          type: 'chat_system',
          text: `${session.nickname} 离开了房间`,
        });

        // If host left, assign new host
        if (session.id === this.hostId && this.sessions.size > 0) {
          const next = this.sessions.values().next().value;
          this.hostId = next.id;
          this.broadcastAll({
            type: 'chat_system',
            text: `${next.nickname} 成为了新房主`,
          });
        }

        this.broadcastMemberList();
      }
    });

    ws.addEventListener('error', () => {
      this.sessions.delete(ws);
    });
  }

  handleMessage(ws, data) {
    const session = this.sessions.get(ws);
    if (!session) return;

    switch (data.type) {
      case 'join':
        session.nickname = data.nickname || '未知';
        // Tell this user their session info + current state
        this.send(ws, {
          type: 'welcome',
          sessionId: session.id,
          isHost: session.id === this.hostId,
          currentBvid: this.currentBvid,
        });
        // Notify others
        this.broadcastExcept(ws, {
          type: 'chat_system',
          text: `${session.nickname} 加入了房间`,
        });
        this.broadcastMemberList();
        break;

      case 'video':
        // Only host can change video (or anyone if no restriction needed)
        this.currentBvid = data.bvid;
        this.broadcastExcept(ws, {
          type: 'video',
          bvid: data.bvid,
        });
        break;

      case 'danmaku':
        this.broadcastExcept(ws, {
          type: 'danmaku',
          text: data.text,
          color: data.color,
        });
        break;

      case 'chat':
        this.broadcastExcept(ws, {
          type: 'chat',
          nick: session.nickname,
          text: data.text,
        });
        break;

      case 'nick_change':
        const oldNick = session.nickname;
        session.nickname = data.nickname || '未知';
        this.broadcastAll({
          type: 'chat_system',
          text: `${oldNick} 改名为 ${session.nickname}`,
        });
        this.broadcastMemberList();
        break;

      case 'ping':
        this.send(ws, { type: 'pong' });
        break;
    }
  }

  send(ws, data) {
    try {
      ws.send(JSON.stringify(data));
    } catch(e) {
      this.sessions.delete(ws);
    }
  }

  broadcastAll(data) {
    const msg = JSON.stringify(data);
    for (const [ws] of this.sessions) {
      try { ws.send(msg); } catch(e) { this.sessions.delete(ws); }
    }
  }

  broadcastExcept(excludeWs, data) {
    const msg = JSON.stringify(data);
    for (const [ws] of this.sessions) {
      if (ws !== excludeWs) {
        try { ws.send(msg); } catch(e) { this.sessions.delete(ws); }
      }
    }
  }

  broadcastMemberList() {
    const list = [];
    for (const [, session] of this.sessions) {
      list.push({
        id: session.id,
        nickname: session.nickname,
        isHost: session.id === this.hostId,
      });
    }
    this.broadcastAll({ type: 'members', list });
  }
}
