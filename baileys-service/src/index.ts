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
const VOLUME_PATH = (process.env.RAILWAY_VOLUME_MOUNT_PATH || '/data').replace(/\/+$/, '') || '/data';
const PERSISTENT_AUTH_DIR = `${VOLUME_PATH}/baileys-auth`;
let activeAuthDir = PERSISTENT_AUTH_DIR;
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
let socketGeneration = 0;
let pairingInProgress = false;
const WHATSAPP_METADATA_CACHE_MS = 60_000;
const COMMUNITY_DISCOVERY_CACHE_MS = 120_000;
let communityDiscoveryCache: { expiresAt: number; value: { communities: any[] } } | null = null;
let communityDiscoveryFetch: Promise<{ communities: any[] }> | null = null;
let participatingGroupsCache: { expiresAt: number; value: Record<string, any> } | null = null;
let participatingGroupsFetch: Promise<Record<string, any>> | null = null;
const groupMetadataCache = new Map<string, { expiresAt: number; value: any }>();

async function getParticipatingGroups(client: WASocket): Promise<Record<string, any>> {
  if (participatingGroupsCache && participatingGroupsCache.expiresAt > Date.now()) return participatingGroupsCache.value;
  if (participatingGroupsFetch) return participatingGroupsFetch;
  const request = client.groupFetchAllParticipating() as Promise<Record<string, any>>;
  participatingGroupsFetch = request;
  try {
    const value = await request;
    participatingGroupsCache = { expiresAt: Date.now() + WHATSAPP_METADATA_CACHE_MS, value };
    return value;
  } finally {
    if (participatingGroupsFetch === request) participatingGroupsFetch = null;
  }
}

async function getGroupMetadata(client: WASocket, groupId: string, forceRefresh = false): Promise<any> {
  const cached = groupMetadataCache.get(groupId);
  if (!forceRefresh && cached && cached.expiresAt > Date.now()) return cached.value;
  const value = await client.groupMetadata(groupId);
  groupMetadataCache.set(groupId, { expiresAt: Date.now() + WHATSAPP_METADATA_CACHE_MS, value });
  return value;
}

function invalidateGroupMetadata(groupId?: string) {
  participatingGroupsCache = null;
  communityDiscoveryCache = null;
  if (groupId) groupMetadataCache.delete(groupId);
  else groupMetadataCache.clear();
}

function applyParticipantUpdateToCachedGroups(update: { id: string; participants?: string[]; action?: string }) {
  const group = participatingGroupsCache?.value?.[update.id];
  if (!group || !Array.isArray(group.participants)) return;
  const incoming = (update.participants || []).map((id) => ({ id, jid: id, phoneNumber: String(id).endsWith('@s.whatsapp.net') ? id : null }));
  const keys = new Set(incoming.map((participant) => jidUser(participant.id)));
  if (update.action === 'add') {
    const existing = new Set(group.participants.map((participant: any) => jidUser(participant.id || participant.jid)));
    group.participants.push(...incoming.filter((participant) => !existing.has(jidUser(participant.id))));
  } else if (update.action === 'remove') {
    group.participants = group.participants.filter((participant: any) => !keys.has(jidUser(participant.id || participant.jid)));
  }
  groupMetadataCache.delete(update.id);
}

let qrExpiresAt: number | null = null;

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

async function hasRegisteredAuth(directory: string): Promise<boolean> {
  try {
    const raw = await fs.readFile(`${directory}/creds.json`, 'utf8');
    return JSON.parse(raw)?.registered === true;
  } catch {
    return false;
  }
}

/**
 * Recover an existing registered session from the persistent volume even if
 * AUTH_DIR was previously configured to a different directory. New sessions
 * always use the Railway volume at /data/baileys-auth.
 */
async function resolveAuthDirectory(): Promise<string> {
  const candidates = [AUTH_DIR, PERSISTENT_AUTH_DIR, VOLUME_PATH];
  const entries = await fs.readdir(VOLUME_PATH, { withFileTypes: true }).catch(() => []);
  for (const entry of entries) {
    if (entry.isDirectory()) candidates.push(`${VOLUME_PATH}/${entry.name}`);
  }
  for (const candidate of [...new Set(candidates)]) {
    if (!await hasRegisteredAuth(candidate)) continue;
    const isPersistent = candidate === VOLUME_PATH || candidate.startsWith(`${VOLUME_PATH}/`);
    if (isPersistent) return candidate;
    await fs.mkdir(PERSISTENT_AUTH_DIR, { recursive: true });
    await fs.cp(candidate, PERSISTENT_AUTH_DIR, { recursive: true, force: true });
    return PERSISTENT_AUTH_DIR;
  }
  return PERSISTENT_AUTH_DIR;
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
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  const generation = ++socketGeneration;

  const operation = (async () => {
    state = 'connecting';
    lastError = null;
    try {
      await fs.mkdir(activeAuthDir, { recursive: true });
      if (forceFresh) {
        await fs.rm(activeAuthDir, { recursive: true, force: true });
        await fs.mkdir(activeAuthDir, { recursive: true });
      }
      const { state: authState, saveCreds } = await useMultiFileAuthState(activeAuthDir);
      const hasRegisteredSession = Boolean(authState.creds.registered);
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
      if (generation !== socketGeneration) {
        try { nextSocket.end(undefined); } catch { /* superseded during socket creation */ }
        return;
      }
      sock = nextSocket;
      // EventEmitter does not await async listeners. Serialize credential writes
      // and wait for them before handling a restart-required (515) close.
      let credentialSaveQueue = Promise.resolve();
      nextSocket.ev.on('creds.update', () => {
        credentialSaveQueue = credentialSaveQueue
          .then(() => saveCreds())
          .catch((error) => {
            logger.error({ error: safeError(error) }, 'Could not persist WhatsApp credentials');
          });
      });
      nextSocket.ev.on('group-participants.update', (update) => {
        // Participant changes do not change the list of joined groups. Keep
        // the expensive participating-groups snapshot warm and patch its
        // participant list locally so the immediate post-add verification
        // does not issue another rate-limited WhatsApp query.
        applyParticipantUpdateToCachedGroups(update);
        logger.info({ groupId: update.id, action: update.action, participantCount: update.participants?.length || 0 }, 'WhatsApp participant update received');
        void emitWebhook('whatsapp.participants_update', {
          group_id: update.id,
          action: update.action,
          participant_count: update.participants?.length || 0,
        });
      });
      nextSocket.ev.on('connection.update', async (update) => {
        if (update.qr) {
          qrText = update.qr;
          qrExpiresAt = Date.now() + 45_000;
          qrDataUrl = await QRCode.toDataURL(update.qr, { margin: 2, width: 320 });
          pairingCode = null;
          state = 'connecting';
          await emitWebhook('whatsapp.qr_ready');
        }
        if (update.connection === 'open') {
          state = 'connected';
          qrText = null;
          qrDataUrl = null;
          qrExpiresAt = null;
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
          if (sock !== nextSocket || generation !== socketGeneration) return;
          // Baileys emits creds.update separately from connection.update. The
          // 515 handshake can close immediately after pair-success, so make
          // sure the new me/keys have reached the mounted volume before restart.
          await credentialSaveQueue;
          if (sock !== nextSocket || generation !== socketGeneration) return;
          sock = null;
          state = loggedOut ? 'logged_out' : 'disconnected';
          qrText = null;
          qrDataUrl = null;
          qrExpiresAt = null;
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
            await fs.rm(activeAuthDir, { recursive: true, force: true }).catch((error) => {
              logger.warn({ error: safeError(error) }, 'Could not clear logged-out WhatsApp credentials');
            });
          } else if (statusCode === 408 && !hasRegisteredSession) {
            // Baileys 408 here means its QR reference attempts expired before
            // pairing. Reconnecting blindly creates an endless QR timeout loop.
            reconnectAllowed = false;
            state = 'disconnected';
            qrText = null;
            qrDataUrl = null;
            qrExpiresAt = null;
            lastError = 'O QR expirou antes da leitura. Solicite um novo QR Code no Superadmin.';
            logger.warn('Unpaired QR session expired; waiting for an explicit new connect request');
          } else if (reconnectAllowed) {
            if (reconnectTimer) clearTimeout(reconnectTimer);
            reconnectAttempts += 1;
            // 515 is Baileys' explicit "restart required" response after a
            // successful pairing handshake. Recreate the socket immediately
            // with the credentials already persisted by creds.update.
            const delayMs = statusCode === 515
              ? 100
              : Math.min(30_000, 3000 * (2 ** Math.min(reconnectAttempts - 1, 4)));
            logger.warn({ reconnectAttempts, reconnectInMs: delayMs, statusCode, restartRequired: statusCode === 515 }, 'Scheduling WhatsApp reconnection');
            reconnectTimer = setTimeout(() => {
              reconnectTimer = null;
              void startSocket();
            }, delayMs);
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

function waitForPairingTransport(client: WASocket, timeoutMs = 45_000): Promise<void> {
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

function isConnectedAccountAdmin(participants: Array<{ id: string; jid?: string; phoneNumber?: string; admin?: string | null }>, client: WASocket): boolean {
  const self = jidUser(client.user?.jid || client.user?.id);
  return Boolean(self && participants.some((participant) =>
    (jidUser(participant.id) === self || jidUser(participant.jid) === self || jidUser(participant.phoneNumber) === self) && Boolean(participant.admin)
  ));
}

/**
 * Baileys 6.7.24 may omit communities from communityFetchAllParticipating()
 * and communityMetadata() can fail for a parent container. Its group parser
 * exposes the same parent reliably through groupFetchAllParticipating().
 */
async function participatingCommunity(client: WASocket, communityId: string): Promise<any | null> {
  const groups = await getParticipatingGroups(client);
  const community = groups[communityId];
  if (!community?.isCommunity || community.isCommunityAnnounce) return null;
  return community;
}

app.get('/health', (_req, res) => res.json({ ok: true, service: 'encontre-plus-baileys', timestamp: new Date().toISOString() }));
app.use('/v1', requireToken);

app.get('/v1/status', (_req, res) => res.json({
  state,
  connected: state === 'connected',
  has_qr: Boolean(qrDataUrl),
  has_pairing_code: Boolean(pairingCode),
  pairing_in_progress: pairingInProgress,
  qr_expires_at: qrExpiresAt ? new Date(qrExpiresAt).toISOString() : null,
  last_error: lastError,
  phone: sock?.user?.id?.split(':')[0] || null,
}));

app.post('/v1/connect', async (_req, res) => {
  try {
    if (pairingInProgress) return res.status(409).json({ error: 'pairing_in_progress', message: 'A vinculação por código já está em andamento.' });
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
  if (pairingInProgress) return res.status(409).json({ error: 'pairing_in_progress', message: 'Já existe uma tentativa de vinculação em andamento. Aguarde ou cancele-a.' });
  if (state === 'connected') {
    return res.status(409).json({ error: 'whatsapp_already_connected', message: 'Use “Ligar outro número” para terminar a sessão actual antes de vincular outro número.' });
  }
  if (state === 'connecting' && sock) {
    return res.status(409).json({ error: 'connection_already_in_progress', message: 'Já existe uma ligação em andamento. Cancele-a antes de mudar o método de ligação.' });
  }

  pairingInProgress = true;
  reconnectAllowed = false;
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  const previousSocket = sock;
  sock = null;
  ++socketGeneration; // invalidate every event handler attached to the previous socket
  try {
    if (previousSocket) {
      try { previousSocket.end(undefined); } catch { /* socket may already be closed */ }
    }
    // Wait for any in-flight socket construction to finish before deleting or
    // replacing credentials. This prevents QR and pairing-code requests racing.
    const pendingStart = startPromise;
    if (pendingStart) await pendingStart.catch(() => undefined);
    await fs.rm(activeAuthDir, { recursive: true, force: true });
    await fs.mkdir(activeAuthDir, { recursive: true });
    state = 'disconnected';
    qrText = null;
    qrDataUrl = null;
    qrExpiresAt = null;
    pairingCode = null;
    lastError = null;

    reconnectAllowed = true;
    await startSocket(true);
    const pairingSocket = currentSocket();
    if (!pairingSocket) throw new Error('O serviço não conseguiu preparar a sessão de emparelhamento. Tente novamente.');
    await waitForPairingTransport(pairingSocket);
    const code = await pairingSocket.requestPairingCode(phone);
    pairingCode = code;
    qrText = null;
    qrDataUrl = null;
    qrExpiresAt = null;
    return res.json({ pairing_code: code, state: 'connecting', instructions: 'Enter this code in WhatsApp > Linked devices > Link with phone number' });
  } catch (error) {
    reconnectAllowed = false;
    state = 'error';
    lastError = safeError(error);
    logger.error({ error: lastError }, 'Could not generate WhatsApp pairing code');
    return res.status(500).json({ error: 'pairing_code_failed', message: lastError });
  } finally {
    pairingInProgress = false;
  }
});

app.get('/v1/groups', async (_req, res) => {
  const client = connectedSocket(res);
  if (!client) return;
  try {
    const groups = await getParticipatingGroups(client);
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
  if (communityDiscoveryCache && communityDiscoveryCache.expiresAt > Date.now()) {
    return res.json(communityDiscoveryCache.value);
  }
  if (communityDiscoveryFetch) {
    try { return res.json(await communityDiscoveryFetch); }
    catch (error) { return res.status(502).json({ error: 'communities_fetch_failed', message: safeError(error) }); }
  }
  const request = (async () => {
    // Discover the parent containers from the one cached participating-groups
    // snapshot. Do not fan out profile-picture and invite-code queries here:
    // this endpoint only populates a selector and those optional requests
    // trigger WhatsApp rate limits when the page is refreshed repeatedly.
    const groups = await getParticipatingGroups(client);
    const groupEntries = Object.values(groups) as any[];
    const discovered = new Map<string, any>();
    for (const group of groupEntries) {
      if (group?.id && group.isCommunity && !group.isCommunityAnnounce) discovered.set(group.id, group);
    }
    const communities = [...discovered.values()].map((community: any) => ({
      id: community.id,
      subject: community.subject || 'Comunidade WhatsApp',
      description: community.desc || null,
      size: community.size || community.participants?.length || 0,
      profile_picture_url: null,
      invite_link: null,
      subgroups: groupEntries
        .filter((group: any) => group.linkedParent === community.id && !group.isCommunityAnnounce)
        .map((group: any) => ({
          id: group.id,
          subject: group.subject || 'Grupo WhatsApp',
          description: group.desc || null,
          size: group.size || group.participants?.length || 0,
          linked_parent_id: group.linkedParent || community.id,
          is_community_announcement: Boolean(group.isCommunityAnnounce),
          profile_picture_url: null,
        })),
    }));
    const value = { communities };
    communityDiscoveryCache = { expiresAt: Date.now() + COMMUNITY_DISCOVERY_CACHE_MS, value };
    logger.info({ detected_communities: communities.length, cached_for_ms: COMMUNITY_DISCOVERY_CACHE_MS }, 'WhatsApp communities discovered');
    return value;
  })();
  communityDiscoveryFetch = request;
  try {
    return res.json(await request);
  } catch (error) {
    logger.warn({ error: safeError(error) }, 'Could not discover WhatsApp communities');
    return res.status(502).json({ error: 'communities_fetch_failed', message: safeError(error) });
  } finally {
    if (communityDiscoveryFetch === request) communityDiscoveryFetch = null;
  }
});

app.get('/v1/communities/:communityId/invite-link', async (req, res) => {
  const client = connectedSocket(res);
  if (!client) return;
  try {
    const community = await participatingCommunity(client, req.params.communityId);
    if (!community) return res.status(404).json({ error: 'whatsapp_community_not_found' });
    if (!isConnectedAccountAdmin(community.participants || [], client)) {
      return res.status(403).json({ error: 'bot_must_be_community_admin' });
    }
    const code = await client.communityInviteCode(req.params.communityId);
    if (!code) return res.status(404).json({ error: 'community_invite_link_unavailable' });
    return res.json({
      community_id: req.params.communityId,
      invite_link: `https://chat.whatsapp.com/${code}`,
    });
  } catch (error) {
    logger.error({ communityId: req.params.communityId, error: safeError(error) }, 'Could not fetch current WhatsApp Community invite');
    return res.status(502).json({ error: 'community_invite_link_fetch_failed', message: safeError(error) });
  }
});

async function participantPhoneJid(client: any, participant: { id: string; jid?: string | null; phoneNumber?: string | null }): Promise<string | null> {
  const phoneNumber = String(participant.phoneNumber || '');
  if (phoneNumber.endsWith('@s.whatsapp.net')) return phoneNumber;
  const supplied = String(participant.jid || participant.id || '');
  if (supplied.endsWith('@s.whatsapp.net')) return supplied;
  if (supplied.endsWith('@lid')) {
    try {
      const mapping = client.signalRepository?.lidMapping;
      const resolver = mapping?.getPNForLID;
      if (typeof resolver === 'function') {
        const resolved = await resolver.call(mapping, supplied);
        if (typeof resolved === 'string' && resolved.endsWith('@s.whatsapp.net')) return resolved;
      }
    } catch (error) {
      logger.debug({ error: safeError(error) }, 'Could not resolve WhatsApp LID to phone JID');
    }
  }
  return null;
}

async function updateParticipantsRaw(client: WASocket & Record<string, any>, jid: string, participants: string[], action: 'add' | 'remove') {
  const query = client.query;
  if (typeof query !== 'function') throw new Error('Baileys query API is unavailable.');
  const response = await query.call(client, {
    tag: 'iq',
    attrs: { type: 'set', xmlns: 'w:g2', to: jid },
    content: [{
      tag: action,
      attrs: {},
      content: participants.map((participant) => ({ tag: 'participant', attrs: { jid: participant } })),
    }],
  });
  const actionNode = Array.isArray(response?.content)
    ? response.content.find((node: any) => node?.tag === action)
    : null;
  const affected = Array.isArray(actionNode?.content)
    ? actionNode.content.filter((node: any) => node?.tag === 'participant')
    : [];
  if (!affected.length) {
    const errorCode = String(actionNode?.attrs?.error || response?.attrs?.error || '');
    throw new Error(errorCode
      ? `WhatsApp rejected participant ${action} (code ${errorCode}).`
      : `WhatsApp returned no participant confirmation for ${action}.`);
  }
  return affected.map((node: any) => ({ status: String(node?.attrs?.error || '200'), jid: String(node?.attrs?.jid || '') }));
}

app.get('/v1/communities/:communityId/participants', async (req, res) => {
  const client = connectedSocket(res) as (WASocket & Record<string, any>) | null;
  if (!client) return;
  try {
    const community = await participatingCommunity(client, req.params.communityId);
    if (!community) return res.status(404).json({ error: 'whatsapp_community_not_found' });
    const participants = await Promise.all((community.participants || []).map(async (participant: { id: string; jid?: string; phoneNumber?: string; admin?: string | null }) => ({
      id: participant.id,
      jid: await participantPhoneJid(client, participant),
      phoneNumber: participant.phoneNumber || null,
      admin: participant.admin || null,
    })));
    return res.json({ community_id: req.params.communityId, participants });
  } catch (error) {
    logger.error({ communityId: req.params.communityId, error: safeError(error) }, 'Could not fetch WhatsApp Community participants');
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
  if (!participant || participant.length > 100 || !/^(?:[0-9]+@s\.whatsapp\.net|[0-9]+@lid)$/.test(participant)) {
    return res.status(400).json({ error: 'invalid_participant_id' });
  }
  try {
    const community = await participatingCommunity(client, req.params.communityId);
    if (!community) return res.status(404).json({ error: 'whatsapp_community_not_found' });
    if (!isConnectedAccountAdmin(community.participants || [], client)) {
      return res.status(403).json({ error: 'bot_must_be_community_admin' });
    }
    const result = await updateParticipantsRaw(client, req.params.communityId, [participant], action);
    if (result.some((item: { status?: string }) => String(item.status) !== '200')) {
      return res.status(502).json({ error: 'community_participant_update_not_confirmed', result });
    }
    applyParticipantUpdateToCachedGroups({ id: req.params.communityId, participants: [participant], action });
    return res.json({ community_id: req.params.communityId, participant, action, result });
  } catch (error) {
    return res.status(502).json({ error: `community_participant_${action}_failed`, message: safeError(error) });
  }
});

app.get('/v1/groups/:groupId/participants', async (req, res) => {
  const client = connectedSocket(res) as (WASocket & Record<string, any>) | null;
  if (!client) return;
  try {
    const metadata = await getGroupMetadata(client, req.params.groupId);
    const participants = await Promise.all(metadata.participants.map(async (participant: { id: string; jid?: string; phoneNumber?: string; admin?: string | null }) => ({
      id: participant.id,
      jid: await participantPhoneJid(client, participant),
      phoneNumber: participant.phoneNumber || null,
      admin: participant.admin || null,
    })));
    return res.json({ group_id: metadata.id, participants });
  } catch (error) {
    return res.status(502).json({ error: 'participants_fetch_failed', message: safeError(error) });
  }
});

app.get('/v1/groups/:groupId/invite-link', async (req, res) => {
  const client = connectedSocket(res);
  if (!client) return;
  try {
    const metadata = await getGroupMetadata(client, req.params.groupId);
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
    const metadata = await getGroupMetadata(client, req.params.groupId, true);
    if (!isConnectedAccountAdmin(metadata.participants, client)) return res.status(403).json({ error: 'bot_must_be_group_admin' });
    const result = await updateParticipantsRaw(client as WASocket & Record<string, any>, req.params.groupId, [participant], 'remove');
    if (result.some((item: { status?: string }) => String(item.status) !== '200')) {
      return res.status(502).json({ error: 'participant_removal_not_confirmed', result });
    }
    invalidateGroupMetadata(req.params.groupId);
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
    const metadata = await getGroupMetadata(client, req.params.groupId, true);
    if (!isConnectedAccountAdmin(metadata.participants, client)) return res.status(403).json({ error: 'bot_must_be_group_admin' });
    const result = await updateParticipantsRaw(client as WASocket & Record<string, any>, req.params.groupId, [phone + '@s.whatsapp.net'], 'add');
    if (result.some((item: { status?: string }) => String(item.status) !== '200')) {
      return res.status(502).json({ error: 'participant_addition_not_confirmed', result });
    }
    invalidateGroupMetadata(req.params.groupId);
    return res.json({ group_id: req.params.groupId, phone, result });
  } catch (error) {
    return res.status(502).json({ error: 'participant_addition_failed', message: safeError(error) });
  }
});

app.post('/v1/messages/send', async (req, res) => {
  const client = connectedSocket(res);
  if (!client) return;
  const phone = String(req.body?.phone || '').replace(/\D/g, '');
  const message = String(req.body?.message || '').trim();
  if (!/^\d{8,15}$/.test(phone)) return res.status(400).json({ error: 'invalid_phone' });
  if (!message || message.length > 3500) return res.status(400).json({ error: 'invalid_message' });
  try {
    const result = await client.sendMessage(phone + '@s.whatsapp.net', { text: message });
    return res.json({ sent: true, message_id: result?.key?.id || null });
  } catch (error) {
    return res.status(502).json({ error: 'whatsapp_message_send_failed', message: safeError(error) });
  }
});

app.post('/v1/disconnect', async (_req, res) => {
  if (pairingInProgress) return res.status(409).json({ error: 'pairing_in_progress', message: 'Aguarde a tentativa de vinculação terminar antes de desconectar.' });
  reconnectAllowed = false;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  reconnectTimer = null;
  ++socketGeneration;
  const currentSocket = sock;
  sock = null;
  try { await currentSocket?.logout(); } catch {
    try { currentSocket?.end(undefined); } catch { /* socket may already be closed */ }
  }
  await fs.rm(activeAuthDir, { recursive: true, force: true });
  state = 'logged_out';
  qrText = null;
  qrDataUrl = null;
  qrExpiresAt = null;
  pairingCode = null;
  lastError = null;
  await emitWebhook('whatsapp.manual_disconnect');
  return res.json({ state, message: 'WhatsApp disconnected and local credentials cleared. A new number can now be linked.' });
});

app.post('/v1/logout', async (_req, res) => {
  if (pairingInProgress) return res.status(409).json({ error: 'pairing_in_progress', message: 'Aguarde a tentativa de vinculação terminar antes de terminar a sessão.' });
  reconnectAllowed = false;
  if (reconnectTimer) clearTimeout(reconnectTimer);
  reconnectTimer = null;
  ++socketGeneration;
  try { await sock?.logout(); } catch { /* continue clearing local credentials */ }
  sock = null;
  state = 'logged_out';
  qrText = null; qrDataUrl = null; qrExpiresAt = null; pairingCode = null;
  await fs.rm(activeAuthDir, { recursive: true, force: true });
  return res.json({ state, message: 'Session logged out and local auth data removed' });
});

app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
  logger.error({ error: safeError(error) }, 'Unhandled request error');
  res.status(500).json({ error: 'internal_server_error' });
});

const server = app.listen(PORT, HOST, () => {
  logger.info({ host: HOST, port: PORT }, 'Encontre + Baileys service listening');
  void resolveAuthDirectory().then(async (resolvedDirectory) => {
    activeAuthDir = resolvedDirectory;
    await fs.mkdir(activeAuthDir, { recursive: true });
    const authEntries = await fs.readdir(activeAuthDir).catch(() => [] as string[]);
    const volumeEntries = await fs.readdir(VOLUME_PATH, { withFileTypes: true }).then(async entries => Promise.all(entries.map(async entry => ({
      name: entry.name,
      files: entry.isDirectory() ? await fs.readdir(`${VOLUME_PATH}/${entry.name}`).catch(() => [] as string[]) : [],
    })))).catch(() => [] as Array<{ name: string; files: string[] }>);
    logger.info({ authDir: activeAuthDir, volumePath: VOLUME_PATH, authEntries, volumeEntries }, 'WhatsApp persistent auth storage diagnostic');
    const { state: authState } = await useMultiFileAuthState(activeAuthDir);
    if (authState.creds.registered) {
      logger.info('Persisted WhatsApp session found; restoring connection');
      await startSocket();
    } else {
      state = 'disconnected';
      logger.info('No paired WhatsApp session found; waiting for an explicit connection request');
    }
  }).catch(error => {
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
