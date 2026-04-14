// ============================================================
// 一起看 - Cloudflare Worker WebSocket 中继服务
// 使用 Durable Objects (Hibernation API) 实现房间隔离
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
// Durable Object: Room (using Hibernation API)
// ============================================================
export class Room {
  constructor(state, env) {
    this.state = state;
    this.hostTag = null;
    this.currentBvid = '';
    this.counter = 0;
  }

  async fetch(request) {
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    const tag = `u${++this.counter}`;

    // Use Hibernation API: acceptWebSocket with tags
    this.state.acceptWebSocket(server, [tag]);

    // Attach metadata via serializeAttachment
    server.serializeAttachment({ id: tag, nickname: '未知' });

    // If first websocket, this user is host
    const allSockets = this.state.getWebSockets();
    if (allSockets.length === 1) {
      this.hostTag = tag;
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  // --- Hibernation API event handlers ---

  async webSocketMessage(ws, message) {
    try {
      const data = JSON.parse(message);
      this.handleMessage(ws, data);
    } catch(e) {
      // ignore invalid JSON
    }
  }

  async webSocketClose(ws, code, reason, wasClean) {
    const meta = ws.deserializeAttachment();
    ws.close(code, reason);

    if (meta) {
      this.broadcastExcept(ws, {
        type: 'chat_system',
        text: `${meta.nickname} 离开了房间`,
      });

      // If host left, assign new host
      if (meta.id === this.hostTag) {
        const remaining = this.state.getWebSockets();
        if (remaining.length > 0) {
          const nextMeta = remaining[0].deserializeAttachment();
          if (nextMeta) {
            this.hostTag = nextMeta.id;
            this.broadcastAll({
              type: 'chat_system',
              text: `${nextMeta.nickname} 成为了新房主`,
            });
          }
        }
      }

      this.broadcastMemberList();
    }
  }

  async webSocketError(ws, error) {
    ws.close(1011, 'WebSocket error');
  }

  // --- Message handling ---

  handleMessage(ws, data) {
    const meta = ws.deserializeAttachment();
    if (!meta) return;

    switch (data.type) {
      case 'join':
        meta.nickname = data.nickname || '未知';
        ws.serializeAttachment(meta);

        this.send(ws, {
          type: 'welcome',
          sessionId: meta.id,
          isHost: meta.id === this.hostTag,
          currentBvid: this.currentBvid,
        });

        this.broadcastExcept(ws, {
          type: 'chat_system',
          text: `${meta.nickname} 加入了房间`,
        });
        this.broadcastMemberList();
        break;

      case 'video':
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
          nick: meta.nickname,
          text: data.text,
        });
        break;

      case 'nick_change': {
        const oldNick = meta.nickname;
        meta.nickname = data.nickname || '未知';
        ws.serializeAttachment(meta);
        this.broadcastAll({
          type: 'chat_system',
          text: `${oldNick} 改名为 ${meta.nickname}`,
        });
        this.broadcastMemberList();
        break;
      }

      case 'ping':
        this.send(ws, { type: 'pong' });
        break;

      // --- WebRTC signaling relay ---
      case 'rtc_offer':
      case 'rtc_answer':
      case 'rtc_ice':
        // Forward to target peer by sessionId
        if (data.target) {
          const targetWs = this.findWsBySessionId(data.target);
          if (targetWs) {
            this.send(targetWs, { ...data, from: meta.id });
          }
        }
        break;

      case 'rtc_join_voice':
        // Notify all others that this user wants voice chat
        this.broadcastExcept(ws, {
          type: 'rtc_join_voice',
          from: meta.id,
          nickname: meta.nickname,
        });
        break;

      case 'rtc_leave_voice':
        this.broadcastExcept(ws, {
          type: 'rtc_leave_voice',
          from: meta.id,
        });
        break;
    }
  }

  findWsBySessionId(sessionId) {
    for (const ws of this.state.getWebSockets()) {
      const meta = ws.deserializeAttachment();
      if (meta && meta.id === sessionId) return ws;
    }
    return null;
  }

  // --- Helpers ---

  send(ws, data) {
    try {
      ws.send(JSON.stringify(data));
    } catch(e) {}
  }

  broadcastAll(data) {
    const msg = JSON.stringify(data);
    for (const ws of this.state.getWebSockets()) {
      try { ws.send(msg); } catch(e) {}
    }
  }

  broadcastExcept(excludeWs, data) {
    const msg = JSON.stringify(data);
    for (const ws of this.state.getWebSockets()) {
      if (ws !== excludeWs) {
        try { ws.send(msg); } catch(e) {}
      }
    }
  }

  broadcastMemberList() {
    const list = [];
    for (const ws of this.state.getWebSockets()) {
      const meta = ws.deserializeAttachment();
      if (meta) {
        list.push({
          id: meta.id,
          nickname: meta.nickname,
          isHost: meta.id === this.hostTag,
        });
      }
    }
    this.broadcastAll({ type: 'members', list });
  }
}
