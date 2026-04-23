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
    // In-memory state (will be restored from storage on wake)
    this.hostTag = null;
    this.currentBvid = '';
    this.videoStartedAt = 0;
    this.playlist = [];
    this.playIndex = -1;
    this.counter = 0;
    this._loaded = false;
  }

  // Restore state from storage (called on hibernation wake)
  async _loadState() {
    if (this._loaded) return;
    this._loaded = true;
    const stored = await this.state.storage.get(['currentBvid', 'videoStartedAt', 'playlist', 'playIndex', 'counter', 'hostTag']);
    if (stored.get('currentBvid') !== undefined) this.currentBvid = stored.get('currentBvid');
    if (stored.get('videoStartedAt') !== undefined) this.videoStartedAt = stored.get('videoStartedAt');
    if (stored.get('playlist') !== undefined) this.playlist = stored.get('playlist');
    if (stored.get('playIndex') !== undefined) this.playIndex = stored.get('playIndex');
    if (stored.get('counter') !== undefined) this.counter = stored.get('counter');
    if (stored.get('hostTag') !== undefined) this.hostTag = stored.get('hostTag');
  }

  // Persist state to storage
  async _saveState() {
    await this.state.storage.put({
      currentBvid: this.currentBvid,
      videoStartedAt: this.videoStartedAt,
      playlist: this.playlist,
      playIndex: this.playIndex,
      counter: this.counter,
      hostTag: this.hostTag,
    });
  }

  async fetch(request) {
    await this._loadState();

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    const tag = `u${++this.counter}`;

    this.state.acceptWebSocket(server, [tag]);
    server.serializeAttachment({ id: tag, nickname: '未知' });

    const allSockets = this.state.getWebSockets();
    if (allSockets.length === 1) {
      this.hostTag = tag;
    }

    await this._saveState();
    return new Response(null, { status: 101, webSocket: client });
  }

  // --- Hibernation API event handlers ---

  async webSocketMessage(ws, message) {
    await this._loadState();
    try {
      const data = JSON.parse(message);
      await this.handleMessage(ws, data);
      await this._saveState();
    } catch(e) {
      // ignore invalid JSON
    }
  }

  async webSocketClose(ws, code, reason, wasClean) {
    await this._loadState();
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
      await this._saveState();
    }
  }

  async webSocketError(ws, error) {
    ws.close(1011, 'WebSocket error');
  }

  // --- Message handling ---

  async handleMessage(ws, data) {
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
          videoStartedAt: this.videoStartedAt,
          serverTime: Date.now(),
          playlist: this.playlist,
          playIndex: this.playIndex,
        });

        this.broadcastExcept(ws, {
          type: 'chat_system',
          text: `${meta.nickname} 加入了房间`,
        });
        this.broadcastMemberList();
        break;

      case 'video':
        this.currentBvid = data.bvid;
        this.videoStartedAt = Date.now();
        this.broadcastExcept(ws, {
          type: 'video',
          bvid: data.bvid,
          videoStartedAt: this.videoStartedAt,
          serverTime: Date.now(),
        });
        break;

      case 'sync_request':
        // New user asks for progress; forward to all others, first reply wins on client
        this.broadcastExcept(ws, {
          type: 'sync_request',
          from: meta.id,
        });
        break;

      case 'sync_reply':
        // Forward progress reply to the requesting peer
        if (data.target) {
          const targetWs = this.findWsBySessionId(data.target);
          if (targetWs) {
            this.send(targetWs, {
              type: 'sync_reply',
              from: meta.id,
              currentTime: data.currentTime,
              platform: data.platform,
            });
          }
        }
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

      case 'emoji':
        this.broadcastExcept(ws, {
          type: 'emoji',
          nick: meta.nickname,
          key: data.key,
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

      case 'playlist_add': {
        // { key, title }
        if (data.key) {
          this.playlist.push({ key: data.key, title: data.title || data.key, addedBy: meta.nickname });
          this.broadcastAll({ type: 'playlist_update', playlist: this.playlist, playIndex: this.playIndex });
          this.broadcastAll({ type: 'chat_system', text: `${meta.nickname} 添加了: ${data.title || data.key}` });
          // If nothing is playing, auto-play the first item
          if (this.playIndex === -1) {
            this.playIndex = 0;
            this.playCurrentItem(meta);
          }
        }
        break;
      }

      case 'playlist_remove': {
        const idx = data.index;
        if (idx >= 0 && idx < this.playlist.length) {
          const removed = this.playlist.splice(idx, 1)[0];
          // Adjust playIndex
          if (idx < this.playIndex) {
            this.playIndex--;
          } else if (idx === this.playIndex) {
            // Current song removed, play next (or stop)
            if (this.playIndex >= this.playlist.length) this.playIndex = this.playlist.length - 1;
            if (this.playIndex >= 0) {
              this.playCurrentItem(meta);
            } else {
              this.currentBvid = '';
              this.videoStartedAt = 0;
            }
          }
          this.broadcastAll({ type: 'playlist_update', playlist: this.playlist, playIndex: this.playIndex });
          this.broadcastAll({ type: 'chat_system', text: `${meta.nickname} 移除了: ${removed.title}` });
        }
        break;
      }

      case 'playlist_play': {
        const idx = data.index;
        if (idx >= 0 && idx < this.playlist.length) {
          this.playIndex = idx;
          this.playCurrentItem(meta);
          this.broadcastAll({ type: 'playlist_update', playlist: this.playlist, playIndex: this.playIndex });
        }
        break;
      }

      case 'playlist_next': {
        if (this.playIndex < this.playlist.length - 1) {
          this.playIndex++;
          this.playCurrentItem(meta);
          this.broadcastAll({ type: 'playlist_update', playlist: this.playlist, playIndex: this.playIndex });
        }
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

      // --- Live stream ---
      case 'live_start':
        this.broadcastExcept(ws, {
          type: 'live_start',
          from: meta.id,
          nickname: meta.nickname,
          mode: data.mode, // 'camera' or 'screen'
        });
        break;

      case 'live_stop':
        this.broadcastExcept(ws, {
          type: 'live_stop',
          from: meta.id,
        });
        break;
    }
  }

  playCurrentItem(meta) {
    if (this.playIndex >= 0 && this.playIndex < this.playlist.length) {
      const item = this.playlist[this.playIndex];
      this.currentBvid = item.key;
      this.videoStartedAt = Date.now();
      this.broadcastAll({
        type: 'video',
        bvid: item.key,
        videoStartedAt: this.videoStartedAt,
        serverTime: Date.now(),
      });
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
