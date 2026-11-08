import 'dotenv/config';
import express, { Request, Response, NextFunction } from 'express';
import cors from 'cors';
import makeWASocket, {
  DisconnectReason,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  WASocket,
  BaileysEventMap,
} from '@whiskeysockets/baileys';
import { Boom } from '@hapi/boom';
import QRCode from 'qrcode';
import pino from 'pino';
import fs from 'node:fs/promises';
import { timingSafeEqual, createHmac } from 'node:crypto';

const app = express();
const logger = pino({ level: process.env.LOG_LEVEL || 'info' });
const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const API_TOKEN = process.env.API_TOKEN || '';
const AUTH_DIR = process.env.AUTH_DIR || '/data/baileys-auth';
const CORS_ORIGINS = (process.env.CORS_ORIGINS || '').split(',').map(v => v.trim()).filter(Boolean);

if (API_TOKEN.length < 32) {
  logger.error('API_TOKEN must contain at least 32 characters. Refusing to start.');
  process.exit(1);
}

app.disable('x-powered-by');
app.use(express.json({ limit: '32kb' }));
app.use(cors({
  origin(origin, callback) {
    if (!origin || CORS_ORIGINS.length === 0 || CORS_ORIGINS.includes(origin)) return callback(null, true);
    return callback(new Error('Origin not allowed'));
  },
  methods: ['GET', 'POST'],
  allowedHeaders: ['Authorization', 'Content-Type'],
}));

type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'logged_out' | 'error';
let sock: WASocket | null = null;
let state: ConnectionState = 'disconnected';
let qrText: string | null = null;
let qrDataUrl: string | null = null;
let pairingCode: string | null = null;
let lastError: string | null = null;
let reconnectTimer: NodeJS.Timeout | null = null;
let startPromise: Promise<void> | null = null;
let reconnectAllowed = true;
let reconnectAttempts = 0;

function requireToken(req: Request, res: Response, next: NextFunction) {
  const header = req.header('authorization') || '';
  const supplied = header.startsWith('Bearer ') ? header.slice(7) : '';
  const a = Buffer.from(supplied);
  const b = Buffer.from(API_TOKEN);
  if (a.length !== b.length || !timingSafeEqual(a, b)) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  next();
}

function safeError(error: unknown): string {
  return error instanceof Error ? error.message.slice(0, 300) : 'Unknown error';
}

async function emitWebhook(event: string, details: Record<string, unknown> = {}) {
  const url = process.env.WEBHOOK_URL;
  if (!url) return;
  try {
    const body = JSON.stringify({ event, occurred_at: new Date().toISOString(), ...details });
    const secret = process.env.WEBHOOK_SECRET || '';
    const signature = createHmac('sha256', secret).update(body).digest('hex');
    await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json', 'x-encontre-signature': signature }, body, signal: AbortSignal.timeout(8000) });
  } catch (error) {
    logger.warn({ error: safeError(error) }, 'Webhook delivery failed');
  }
}

async function startSocket(forceFresh = false): Promise<void> {
  if (startPromise) return startPromise;
  if (sock && (state === 'connecting' || state === 'connected')) return;

  const operation = (async () => {
    state = 'connecting';
    lastError = null;
    try {
      await fs.mkdir(AUTH_DIR, { recursive: true });
      if (forceFresh) {
        await fs.rm(AUTH_DIR, { recursive: true, force: true });
        await fs.mkdir(AUTH_DIR, { recursive: true });
      }
      const { state: authState, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
      const { version } = await fetchLatestBaileysVersion();
      const nextSocket = makeWASocket({
        version,
        auth: authState,
        logger: logger.child({ component: 'baileys' }),
        printQRInTerminal: false,
        markOnlineOnConnect: false,
        syncFullHistory: false,
        generateHighQualityLinkPreview: false,
        connectTimeoutMs: 60_000,
        defaultQueryTimeoutMs: 60_000,
      });
      sock = nextSocket;
      nextSocket.ev.on('creds.update', saveCreds);
      nextSocket.ev.on('connection.update', async (update) => {
        if (update.qr) {
          qrText = update.qr;
          qrDataUrl = await QRCode.toDataURL(update.qr, { margin: 2, width: 320 });
          pairingCode = null;
          state = 'connecting';
          await emitWebhook('whatsapp.qr_ready');
        }
        if (update.connection === 'open') {
          state = 'connected';
          qrText = null;
          qrDataUrl = null;
          pairingCode = null;
          lastError = null;
          reconnectAttempts = 0;
          reconnectTimer = null;
          logger.info('WhatsApp session connected');
          await emitWebhook('whatsapp.connected');
        }
        if (update.connection === 'close') {
          const statusCode = (update.lastDisconnect?.error as Boom | undefined)?.output?.statusCode;
          const loggedOut = statusCode === DisconnectReason.loggedOut;
          // Ignore close events from sockets that were intentionally replaced.
          if (sock !== nextSocket) return;
          sock = null;
          state = loggedOut ? 'logged_out' : 'disconnected';
          qrText = null;
          qrDataUrl = null;
          pairingCode = null;
          lastError = statusCode === 403
            ? 'WhatsApp recusou a sessão (403); pode ser necessário voltar a vincular o dispositivo.'
            : statusCode
              ? `WhatsApp desconectou (código ${statusCode}).`
              : 'WhatsApp desconectou sem informar um código.';
          logger.warn({ statusCode, loggedOut }, 'WhatsApp connection closed');
          await emitWebhook('whatsapp.disconnected', { logged_out: loggedOut, status_code: statusCode });
          if (loggedOut) {
            // A logged-out session cannot be reused to link another number.
            await fs.rm(AUTH_DIR, { recursive: true, force: true }).catch((error) => {
              logger.warn({ error: safeError(error) }, 'Could not clear logged-out WhatsApp credentials');
            });
          } else if (reconnectAllowed) {
            if (reconnectTimer) clearTimeout(reconnectTimer);
            reconnectAttempts += 1;
            const delayMs = Math.min(5 * 60 * 1000, 5000 * (2 ** Math.min(reconnectAttempts - 1, 6)));
            logger.warn({ reconnectAttempts, reconnectInMs: delayMs, statusCode }, 'Scheduling WhatsApp reconnection');
            reconnectTimer = setTimeout(() => void startSocket(), delayMs);
          }
        }
      });
    } catch (error) {
      state = 'error';
      lastError = safeError(error);
      logger.error({ error: lastError }, 'Could not start WhatsApp socket');
      throw error;
    }
  })();

  startPromise = operation;
  try {
    await operation;
  } finally {
    if (startPromise === operation) startPromise = null;
  }
}

function waitForPairingTransport(client: WASocket, timeoutMs = 20_000): Promise<void> {
  if (qrText || state === 'connected') return Promise.resolve();
  return new Promise((resolve, reject) => {
    const finish = (error?: Error) => {
      clearTimeout(timeout);
      client.ev.off('connection.update', onUpdate);
      if (error) reject(error);
      else resolve();
    };
    const onUpdate = (update: BaileysEventMap['connection.update']) => {
      if (update.qr || update.connection === 'open') finish();
      else if (update.connection === 'close') {
        const statusCode = (update.lastDisconnect?.error as Boom | undefined)?.output?.statusCode;
        finish(new Error(`WhatsApp closed the connection before pairing was ready (status ${statusCode || 'unknown'}).`));
      }
    };
    const timeout = setTimeout(() => finish(new Error('Timed out waiting for WhatsApp pairing transport.')), timeoutMs);
    client.ev.on('connection.update', onUpdate);
  });
}

function currentSocket(): WASocket | null {
  return sock;
}

function connectedSocket(res: Response): WASocket | null {
  if (!sock || state !== 'connected') {
    res.status(409).json({ error: 'whatsapp_not_connected', state });
    return null;
  }
  return sock;
}

function jidUser(value: string | undefined): string {
  return String(value || '').split(':')[0].split('@')[0];
}

function isConnectedAccountAdmin(participants: Array<{ id: string; admin?: string | null }>, client: WASocket): boolean {
  const self = jidUser(client.user?.id);
  return Boolean(self && participants.some((participant) => jidUser(participant.id) === self && Boolean(participant.admin)));
}

app.get('/health', (_req, res) => res.json({ ok: true, service: 'encontre-plus-baileys', timestamp: new Date().toISOString() }));
app.use('/v1', requireToken);

app.get('/v1/status', (_req, res) => res.json({
  state,
  connected: state === 'connected',
  has_qr: Boolean(qrDataUrl),
  has_pairing_code: Boolean(pairingCode),
  last_error: lastError,
  phone: sock?.user?.id?.split(':')[0] || null,
}));

app.post('/v1/connect', async (_req, res) => {
  try {
    if (state === 'connected') return res.json({ state, message: 'Already connected' });
    if (state === 'connecting' && sock) return res.status(202).json({ state: 'connecting', message: 'Connection already in progress; poll /v1/qr or /v1/status' });
    reconnectAllowed = true;
    await startSocket(state === 'logged_out');
    return res.status(202).json({ state: 'connecting', message: 'Connection started; poll /v1/qr or /v1/status' });
  } catch (error) {
    return res.status(500).json({ error: 'connection_start_failed', message: safeError(error) });
  }
});

app.get('/v1/qr', (_req, res) => {
  if (!qrDataUrl) return res.status(404).json({ error: 'qr_not_ready', state });
  return res.json({ qr_data_url: qrDataUrl, expires_in_seconds: 45, state });
});

app.post('/v1/pairing-code', async (req, res) => {
  const phone = String(req.body?.phone || '').replace(/\D/g, '');
  if (!/^\d{8,15}$/.test(phone)) return res.status(400).json({ error: 'phone_must_include_country_code_digits_only' });
  try {
    if (state === 'connected') {
      return res.status(409).json({ error: 'whatsapp_already_connected', message: 'Desconecte a conta atual antes de vincular outro número.' });
    }

    // Pairing a different number must never reuse credentials from the previous account.
    reconnectAllowed = false;
    if (reconnectTimer) clearTimeout(reconnectTimer);
    const previousSocket = sock;
    sock = null;
    if (previousSocket) {
      try { previousSocket.end(undefined); } catch { /* socket may already be closed */ }
    }
    qrText = null;
    qrDataUrl = null;
    pairingCode = null;
    await fs.rm(AUTH_DIR, { recursive: true, force: true });
    await startSocket(true);

    const pairingSocket = currentSocket();
    if (!pairingSocket) return res.status(503).json({ error: 'socket_not_ready' });
    reconnectAllowed = true;
    await waitForPairingTransport(pairingSocket);
    const code = await pairingSocket.requestPairingCode(phone);
    pairingCode = code;
    qrText = null;
    qrDataUrl = null;
    return res.json({ pairing_code: code, state: 'connecting', instructions: 'Enter this code in WhatsApp > Linked devices > Link with phone number' });
  } catch (error) {
    lastError = safeError(error);
    logger.error({ error: lastError }, 'Could not generate WhatsApp pairing code');
    return res.status(500).json({ error: 'pairing_code_failed', message: lastError });
  }
});

app.get('/v1/groups', async (_req, res) => {
  const client = connectedSocket(res);
  if (!client) return;
  try {
    const groups = await client.groupFetchAllParticipating();
    return res.json({ groups: Object.values(groups).map(group => ({
      id: group.id, subject: group.subject, description: group.desc || null,
      owner: group.owner || null, size: group.participants?.length || 0,
    })) });
  } catch (error) {
    return res.status(502).json({ error: 'groups_fetch_failed', message: safeError(error) });
  }
});

app.get('/v1/communities', async (_req, res) => {
  const client = connectedSocket(res) as any;
  if (!client) return;
  try {
    const [communities, groups] = await Promise.all([
      client.communityFetchAllParticipating(),
      client.groupFetchAllParticipating(),
    ]);
    const communityEntries = Object.values(communities) as any[];
    const groupEntries = Object.values(groups) as any[];

    // Baileys 6.7.24 can return an empty "communities" collection because
    // its community parser expects <communities>/<community>, while WhatsApp
    // may return the parent container as a normal <group>. The group parser
    // exposes that container reliably through isCommunity.
    const discoveredCommunities = new Map<string, any>();
    for (const community of communityEntries) {
      if (community?.id) discoveredCommunities.set(community.id, community);
    }
    for (const group of groupEntries) {
      if (group?.id && group.isCommunity && !group.isCommunityAnnounce) {
        discoveredCommunities.set(group.id, group);
      }
    }
    const allCommunities = [...discoveredCommunities.values()];
    logger.info({
      community_api_results: communityEntries.length,
      group_api_results: groupEntries.length,
      detected_communities: allCommunities.length,
    }, 'WhatsApp communities discovered');

    const result = await Promise.all(allCommunities.map(async (community: any) => {
      const linkedGroups = groupEntries.filter((group: any) =>
        group.linkedParent === community.id && !group.isCommunityAnnounce
      );
      const [imageUrl, inviteCode, subgroups] = await Promise.all([
        client.profilePictureUrl(community.id, 'image').catch(() => null),
        client.communityInviteCode(community.id).catch(() => null),
        Promise.all(linkedGroups.map(async group => ({
          id: group.id,
          subject: group.subject,
          description: group.desc || null,
          size: group.size || group.participants?.length || 0,
          linked_parent_id: group.linkedParent || community.id,
          is_community_announcement: Boolean(group.isCommunityAnnounce),
          profile_picture_url: await client.profilePictureUrl(group.id, 'image').catch(() => null),
        }))),
      ]);
      return {
        id: community.id,
        subject: community.subject,
        description: community.desc || null,
        size: community.size || community.participants?.length || 0,
        profile_picture_url: imageUrl,
        invite_link: inviteCode ? `https://chat.whatsapp.com/${inviteCode}` : null,
        subgroups,
      };
    }));
    return res.json({ communities: result });
  } catch (error) {
    return res.status(502).json({ error: 'communities_fetch_failed', message: safeError(error) });
  }
});

app.get('/v1/communities/:communityId/participants', async (req, res) => {
  const client = connectedSocket(res) as (WASocket & Record<string, any>) | null;
  if (!client) return;
  try {
    const metadata = await client.communityMetadata(req.params.communityId);
    return res.json({
      community_id: req.params.communityId,
      participants: (metadata.participants || []).map((participant: { id: string; admin?: string | null }) => ({
        id: participant.id,
        admin: participant.admin || null,
      })),
    });
  } catch (error) {
    return res.status(502).json({ error: 'community_participants_fetch_failed', message: safeError(error) });
  }
});

app.post('/v1/communities/:communityId/participants/:action', async (req, res) => {
  const client = connectedSocket(res) as (WASocket & Record<string, any>) | null;
  if (!client) return;
  const action = req.params.action;
  if (action !== 'add' && action !== 'remove') {
    return res.status(400).json({ error: 'invalid_community_participant_action' });
  }
  const phone = String(req.body?.phone || '').replace(/\D/g, '');
  const participant = action === 'remove' ? String(req.body?.participant || '') : `${phone}@s.whatsapp.net`;
  if (!participant || participant.length > 100 || !/^[0-9]+@s\.whatsapp\.net$/.test(participant)) {
    return res.status(400).json({ error: 'invalid_participant_id' });
  }
  try {
    const metadata = await client.communityMetadata(req.params.communityId);
    if (!isConnectedAccountAdmin(metadata.participants, client)) {
      return res.status(403).json({ error: 'bot_must_be_community_admin' });
    }
    const result = await client.communityParticipantsUpdate(req.params.communityId, [participant], action);
    if (result.some((item: { status?: string }) => String(item.status) !== '200')) {
      return res.status(502).json({ error: 'community_participant_update_not_confirmed', result });
    }
    return res.json({ community_id: req.params.communityId, participant, action, result });
  } catch (error) {
    return res.status(502).json({ error: `community_participant_${action}_failed`, message: safeError(error) });
  }
});

app.get('/v1/groups/:groupId/participants', async (req, res) => {
  const client = connectedSocket(res);
  if (!client) return;
  try {
    const metadata = await client.groupMetadata(req.params.groupId);
    return res.json({ group_id: metadata.id, participants: metadata.participants.map(p => ({
      id: p.id, admin: p.admin || null,
    })) });
  } catch (error) {
    return res.status(502).json({ error: 'participants_fetch_failed', message: safeError(error) });
  }
});

app.get('/v1/groups/:groupId/invite-link', async (req, res) => {
  const client = connectedSocket(res);
  if (!client) return;
  try {
    const metadata = await client.groupMetadata(req.params.groupId);
    if (!isConnectedAccountAdmin(metadata.participants, client)) {
      return res.status(403).json({ error: 'bot_must_be_group_admin' });
    }
    const code = await client.groupInviteCode(req.params.groupId);
    if (!code) return res.status(404).json({ error: 'group_invite_link_unavailable' });
    return res.json({ group_id: req.params.groupId, invite_link: `https://chat.whatsapp.com/${code}` });
  } catch (error) {
    return res.status(502).json({ error: 'invite_link_fetch_failed', message: safeError(error) });
  }
});

app.post('/v1/groups/:groupId/participants/remove', async (req, res) => {
  const client = connectedSocket(res);
  if (!client) return;
  const participant = String(req.body?.participant || '');
  if (!participant || participant.length > 100 || !/^\d+(@s\.whatsapp\.net|@lid)$/.test(participant)) {
    return res.status(400).json({ error: 'invalid_participant_id' });
  }
  try {
    const metadata = await client.groupMetadata(req.params.groupId);
    if (!isConnectedAccountAdmin(metadata.participants, client)) return res.status(403).json({ error: 'bot_must_be_group_admin' });
    const result = await client.groupParticipantsUpdate(req.params.groupId, [participant], 'remove');
    if (result.some((item) => String(item.status) !== '200')) {
      return res.status(502).json({ error: 'participant_removal_not_confirmed', result });
    }
    return res.json({ group_id: req.params.groupId, participant, result });
  } catch (error) {
    return res.status(502).json({ error: 'participant_removal_failed', message: safeError(error) });
  }
});

app.post('/v1/groups/:groupId/participants/add', async (req, res) => {
  const client = connectedSocket(res);
  if (!client) return;
  const phone = String(req.body?.phone || '').replace(/\D/g, '');
  if (!/^\d{8,15}$/.test(phone)) return res.status(400).json({ error: 'invalid_phone' });
  try {
    const metadata = await client.groupMetadata(req.params.groupId);
    if (!isConnectedAccountAdmin(metadata.participants, client)) return res.status(403).json({ error: 'bot_must_be_group_admin' });
    const result = await client.groupParticipantsUpdate(req.params.groupId, [phone + '@s.whatsapp.net'], 'add');
    if (result.some((item) => String(item.status) !== '200')) {
      return res.status(502).json({ error: 'participant_addition_not_confirmed', result });
    }
    return res.json({ group_id: req.params.groupId, phone, result });
  } catch (error) {
    return res.status(502).json({ error: 'participant_addition_failed', message: safeError(error) });
  }
});

app.post('/v1/disconnect', async (_req, res) => {
  reconnectAllowed = false;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  const currentSocket = sock;
  sock = null;
  try { await currentSocket?.logout(); } catch {
    try { currentSocket?.end(undefined); } catch { /* socket may already be closed */ }
  }
  await fs.rm(AUTH_DIR, { recursive: true, force: true });
  state = 'logged_out';
  qrText = null;
  qrDataUrl = null;
  pairingCode = null;
  lastError = null;
  await emitWebhook('whatsapp.manual_disconnect');
  return res.json({ state, message: 'WhatsApp disconnected and local credentials cleared. A new number can now be linked.' });
});

app.post('/v1/logout', async (_req, res) => {
  reconnectAllowed = false;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  try { await sock?.logout(); } catch { /* continue clearing local credentials */ }
  sock = null;
  state = 'logged_out';
  qrText = null; qrDataUrl = null; pairingCode = null;
  await fs.rm(AUTH_DIR, { recursive: true, force: true });
  return res.json({ state, message: 'Session logged out and local auth data removed' });
});

app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
  logger.error({ error: safeError(error) }, 'Unhandled request error');
  res.status(500).json({ error: 'internal_server_error' });
});

const server = app.listen(PORT, HOST, () => {
  logger.info({ host: HOST, port: PORT }, 'Encontre + Baileys service listening');
  void fs.mkdir(AUTH_DIR, { recursive: true }).then(() => startSocket()).catch(error => {
    logger.error({ error: safeError(error) }, 'Initial WhatsApp startup failed; use /v1/connect to retry');
  });
});

process.on('SIGTERM', () => {
  reconnectAllowed = false;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  try { sock?.end(undefined); } catch { /* ignore */ }
  server.close(() => process.exit(0));
});
process.on('SIGINT', () => {
  reconnectAllowed = false;
  try { sock?.end(undefined); } catch { /* ignore */ }
  server.close(() => process.exit(0));
});
