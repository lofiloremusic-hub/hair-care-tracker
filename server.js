
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { OpenAI } = require('openai');
const https = require('https');
const http = require('http');
const webpush = require('web-push');
const admin = require('firebase-admin');
const { DateTime } = require('luxon');

const app = express();
const PORT = process.env.PORT || 3000;

const allowedOrigins = [
  'https://jordyn-haircare.web.app',
  'https://jordyn-haircare.firebaseapp.com',
  'https://hair-care-tracker.web.app',
  'http://localhost:5000',
  'http://127.0.0.1:5500'
];

app.use(cors({
  origin: function(origin, callback) {
    if (!origin || allowedOrigins.includes(origin)) return callback(null, true);
    return callback(new Error('Not allowed by CORS'));
  },
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use(express.json({ limit: '40kb' }));

if (!process.env.OPENAI_API_KEY) {
  console.error('OPENAI_API_KEY is not set.');
  process.exit(1);
}

if (!process.env.VAPID_PUBLIC_KEY || !process.env.VAPID_PRIVATE_KEY || !process.env.VAPID_SUBJECT) {
  console.error('VAPID keys are not set.');
  process.exit(1);
}

if (!process.env.FIREBASE_SERVICE_ACCOUNT_JSON) {
  console.error('FIREBASE_SERVICE_ACCOUNT_JSON is not set.');
  process.exit(1);
}

webpush.setVapidDetails(
  process.env.VAPID_SUBJECT,
  process.env.VAPID_PUBLIC_KEY,
  process.env.VAPID_PRIVATE_KEY
);

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT_JSON);
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount)
});

const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;
const FieldPath = admin.firestore.FieldPath;

const openai = new OpenAI({
  baseURL: 'https://openrouter.ai/api/v1',
  apiKey: process.env.OPENAI_API_KEY,
  defaultHeaders: {
    'HTTP-Referer': 'https://jordyn-haircare.web.app',
    'X-Title': 'Jordyn Haircare'
  }
});

const SMART_NOTIFICATION_SLOTS = [
  { key: 'morning', hour: 10, minute: 0 },
  { key: 'afternoon', hour: 15, minute: 0 },
  { key: 'evening', hour: 17, minute: 0 }
];

const AI_LIMITS = {
  advancedPerDay: 30,
  freePerMonth: 30
};

const MAX_AI_HISTORY_ITEMS = 12;
const SCHEDULE_CATCHUP_MINUTES = Math.max(12, parseInt(process.env.SCHEDULE_CATCHUP_MINUTES || '24', 10) || 24);
const SCHEDULE_SWEEP_LIMIT = Math.max(50, Math.min(500, parseInt(process.env.SCHEDULE_SWEEP_LIMIT || '250', 10) || 250));
const BROADCAST_BATCH_SIZE = Math.max(10, Math.min(100, parseInt(process.env.BROADCAST_BATCH_SIZE || '40', 10) || 40));
const ADMIN_EMAILS = new Set(['iamkaransingh0709@gmail.com', 'jordynjada03@gmail.com']);

let lastScheduleSweepAt = 0;
let scheduleCursorId = '';
let scheduleRunning = false;
let broadcastRunning = false;
let lastBroadcastProcessedId = '';

const REMINDER_TYPE_EMOJI = {
  wash: '🧼',
  condition: '💆',
  protein: '💪',
  oil: '🌿',
  trim: '✂️',
  protective: '🌸',
  hydrate: '💧',
  custom: '🔔'
};

function cleanSubscription(subscription) {
  if (!subscription || !subscription.endpoint || !subscription.keys) return null;
  return {
    endpoint: subscription.endpoint,
    expirationTime: subscription.expirationTime || null,
    keys: {
      p256dh: subscription.keys.p256dh,
      auth: subscription.keys.auth
    }
  };
}

function getTodayKey(zone) {
  return DateTime.now().setZone(zone || 'UTC').toFormat('yyyy-LL-dd');
}

function getNotificationName(userDoc, userData) {
  const name = ((userData && userData.profile && userData.profile.name) || userDoc.displayName || 'love').trim();
  return name.split(' ')[0] || 'love';
}

function getReminderIntervalDays(freq) {
  if (freq === 'once') return 0;
  if (freq === 'daily') return 1;
  if (freq === 'every3days') return 3;
  if (freq === 'every5days') return 5;
  if (freq === 'weekly') return 7;
  if (freq === 'biweekly') return 14;
  if (freq === 'monthly') return 30;
  if (freq === 'quarterly') return 90;
  return 0;
}

function getReminderBaseDate(reminder, localNow) {
  const ds = reminder.date || reminder.startDate;
  if (ds) {
    const parsed = DateTime.fromISO(String(ds), { zone: localNow.zoneName });
    if (parsed.isValid) return parsed;
  }
  if (reminder.ts) return DateTime.fromMillis(reminder.ts).setZone(localNow.zoneName);
  return localNow.startOf('day');
}

function isReminderDueToday(reminder, localNow) {
  const freq = reminder.frequency || 'once';
  const todayStart = localNow.startOf('day');
  const base = getReminderBaseDate(reminder, localNow);
  const baseStart = base.startOf('day');

  if ((reminder.frequency || 'once') === 'once') {
    return baseStart.toFormat('yyyy-LL-dd') === todayStart.toFormat('yyyy-LL-dd');
  }

  if (freq === 'monthly' || freq === 'quarterly') {
    const monthsSince = (todayStart.year - baseStart.year) * 12 + (todayStart.month - baseStart.month);
    if (monthsSince < 0 || todayStart.day !== baseStart.day) return false;
    return freq === 'monthly' ? true : monthsSince % 3 === 0;
  }

  const interval = getReminderIntervalDays(freq);
  if (!interval) return false;
  const daysSince = Math.floor(todayStart.diff(baseStart, 'days').days);
  return daysSince >= 0 && daysSince % interval === 0;
}

function buildReminderNotification(reminder, userDoc, userData) {
  const name = getNotificationName(userDoc, userData);
  const emoji = REMINDER_TYPE_EMOJI[reminder.type] || '🔔';
  return {
    title: `${emoji} ${reminder.title} for ${name}`,
    body: 'Your scheduled hair reminder is ready. Tap to open Jordyn and stay consistent.',
    tag: `jhb-rem-${reminder.id}`,
    data: { page: 'Calendar', url: '/#open-page=Calendar', reminderId: reminder.id }
  };
}

function buildSmartNotification(slotKey, userDoc, userData) {
  const name = getNotificationName(userDoc, userData);
  const profile = (userData && userData.profile) || {};
  const goal = profile.hairGoal || 'healthier hair';
  if (slotKey === 'morning') {
    return {
      title: `✨ Morning hair note for ${name}`,
      body: `Start the day with a small step toward ${goal.toLowerCase()}. Keep moisture in early and protect your ends.`,
      tag: 'jhb-morning',
      data: { page: 'Home', url: '/#open-page=Home' }
    };
  }
  if (slotKey === 'afternoon') {
    return {
      title: `💡 Midday hair tip for ${name}`,
      body: 'Quick reset: smooth dry areas, keep styling low-tension, and avoid over-touching your hair this afternoon.',
      tag: 'jhb-afternoon',
      data: { page: 'Home', url: '/#open-page=Home' }
    };
  }
  return {
    title: `🌙 Evening routine reminder for ${name}`,
    body: 'Protect your progress tonight with one calm hair-care step before the day ends.',
    tag: 'jhb-evening',
    data: { page: 'Calendar', url: '/#open-page=Calendar' }
  };
}

async function sendToSubscriptions(subscriptions, payload) {
  const alive = [];
  for (const sub of subscriptions || []) {
    try {
      await webpush.sendNotification(sub, JSON.stringify(payload));
      alive.push(sub);
    } catch (err) {
      const code = err && err.statusCode;
      if (code !== 404 && code !== 410) alive.push(sub);
    }
  }
  return alive;
}

function uniqueByEndpoint(list) {
  const seen = new Set();
  return (list || []).filter((sub) => {
    if (!sub || !sub.endpoint || seen.has(sub.endpoint)) return false;
    seen.add(sub.endpoint);
    return true;
  });
}

function jsonError(res, status, code, message, extra) {
  return res.status(status).json(Object.assign({
    code,
    error: message || code
  }, extra || {}));
}

function getValidTimezone(requestedZone) {
  const zone = requestedZone || 'UTC';
  return DateTime.now().setZone(zone).isValid ? zone : 'UTC';
}

async function mirrorNotificationPreferenceToUserData(uid, enabled, timezone) {
  const profilePatch = {
    notifications: !!enabled,
    pushLinked: !!enabled,
    timezone: getValidTimezone(timezone)
  };
  if (enabled) profilePatch.notificationPermission = 'granted';

  try {
    await db.collection('userData').doc(uid).set({
      profile: profilePatch,
      syncMeta: {
        collections: {
          profile: Date.now()
        }
      },
      updatedAt: FieldValue.serverTimestamp()
    }, { merge: true });
  } catch (err) {
    console.warn('Could not mirror notification preference to userData:', err.message);
  }
}

async function requireFirebaseAuth(req, res) {
  const header = String(req.headers.authorization || '');
  if (!header.startsWith('Bearer ')) {
    jsonError(res, 401, 'auth_required', 'Missing session token.');
    return null;
  }

  const token = header.slice(7).trim();
  if (!token) {
    jsonError(res, 401, 'auth_required', 'Missing session token.');
    return null;
  }

  try {
    return await admin.auth().verifyIdToken(token);
  } catch (err) {
    console.error('Auth verification failed:', err.message);
    jsonError(res, 401, 'auth_invalid', 'Your session expired. Please sign in again.');
    return null;
  }
}

async function requireAdminAuth(req, res) {
  const auth = await requireFirebaseAuth(req, res);
  if (!auth) return null;

  const email = String(auth.email || '').toLowerCase();
  if (ADMIN_EMAILS.has(email)) return auth;

  try {
    const snap = await db.collection('users').doc(auth.uid).get();
    const userDoc = snap.exists ? (snap.data() || {}) : {};
    if (userDoc.isAdmin === true) return auth;
  } catch (err) {
    console.error('Admin lookup failed:', err.message);
  }

  jsonError(res, 403, 'admin_required', 'Admin access required.');
  return null;
}

function sanitizeMessageText(value, maxLength) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, maxLength);
}

function sanitizeAIHistory(history) {
  if (!Array.isArray(history)) return [];
  return history.slice(-MAX_AI_HISTORY_ITEMS).map((item) => ({
    role: item && item.role === 'ai' ? 'assistant' : 'user',
    content: sanitizeMessageText(item && item.text, 1200)
  })).filter((item) => item.content);
}

function pruneKeyedLog(log, keepKey) {
  const next = {};
  const used = parseInt((log || {})[keepKey] || 0, 10) || 0;
  next[keepKey] = used;
  return next;
}

function getServerAIQuotaState(userDoc, nowUtc) {
  const zone = getValidTimezone(userDoc.timezone);
  const localNow = nowUtc.setZone(zone);
  const isAdvanced = !!(userDoc.isAdmin || userDoc.isPremium);
  return {
    isAdvanced,
    limit: isAdvanced ? AI_LIMITS.advancedPerDay : AI_LIMITS.freePerMonth,
    periodKey: isAdvanced ? localNow.toFormat('yyyy-LL-dd') : localNow.toFormat('yyyy-LL')
  };
}

function formatQuotaResponse(quota) {
  return {
    limit: quota.limit,
    used: quota.used,
    remaining: quota.remaining,
    periodKey: quota.periodKey,
    mode: quota.isAdvanced ? 'advanced' : 'standard'
  };
}

async function consumeServerAIQuota(userRef, nowUtc, requestedTier) {
  return db.runTransaction(async (tx) => {
    const snap = await tx.get(userRef);
    const userDoc = snap.exists ? (snap.data() || {}) : {};
    const quota = getServerAIQuotaState(userDoc, nowUtc);
    const log = pruneKeyedLog(userDoc.aiUsageLog || {}, quota.periodKey);
    const used = parseInt(log[quota.periodKey] || 0, 10) || 0;

    if (requestedTier === 'advanced' && !quota.isAdvanced) {
      return Object.assign({ allowed: false, reason: 'advanced_ai_locked', used, remaining: Math.max(0, quota.limit - used) }, quota, { userDoc });
    }

    if (used >= quota.limit) {
      return Object.assign({ allowed: false, reason: 'quota_exceeded', used, remaining: 0 }, quota, { userDoc });
    }

    log[quota.periodKey] = used + 1;
    tx.set(userRef, {
      aiUsageLog: log,
      updatedAt: FieldValue.serverTimestamp()
    }, { merge: true });

    return Object.assign({ allowed: true, used: used + 1, remaining: quota.limit - (used + 1) }, quota, { userDoc });
  });
}

async function refundServerAIQuota(userRef, periodKey) {
  if (!periodKey) return;
  try {
    await db.runTransaction(async (tx) => {
      const snap = await tx.get(userRef);
      if (!snap.exists) return;
      const userDoc = snap.data() || {};
      const log = Object.assign({}, userDoc.aiUsageLog || {});
      const used = parseInt(log[periodKey] || 0, 10) || 0;
      if (used <= 0) return;
      log[periodKey] = used - 1;
      tx.set(userRef, {
        aiUsageLog: log,
        updatedAt: FieldValue.serverTimestamp()
      }, { merge: true });
    });
  } catch (err) {
    console.warn('AI quota refund failed:', err.message);
  }
}

function getCatchupWindowStart(nowUtc, lastRunAt) {
  if (!lastRunAt) return nowUtc.minus({ minutes: SCHEDULE_CATCHUP_MINUTES });
  const previous = DateTime.fromMillis(lastRunAt).toUTC();
  if (!previous.isValid) return nowUtc.minus({ minutes: SCHEDULE_CATCHUP_MINUTES });
  const floor = nowUtc.minus({ minutes: SCHEDULE_CATCHUP_MINUTES });
  return previous < floor ? floor : previous.minus({ seconds: 2 });
}

function getCandidateLocalDays(windowStartLocal, windowEndLocal) {
  const keys = new Set();
  const days = [];

  [windowStartLocal.startOf('day'), windowEndLocal.startOf('day')].forEach((day) => {
    const key = day.toFormat('yyyy-LL-dd');
    if (!keys.has(key)) {
      keys.add(key);
      days.push(day);
    }
  });

  return days;
}

function findDueLocalMomentForClock(hour, minute, windowStartLocal, windowEndLocal) {
  const candidateDays = getCandidateLocalDays(windowStartLocal, windowEndLocal);
  for (const day of candidateDays) {
    const target = day.set({ hour, minute, second: 0, millisecond: 0 });
    if (target >= windowStartLocal && target <= windowEndLocal) return target;
  }
  return null;
}

function findDueReminderMoment(reminder, windowStartLocal, windowEndLocal) {
  const hm = String(reminder.time || '08:00').slice(0, 5).split(':');
  const hour = parseInt(hm[0], 10) || 0;
  const minute = parseInt(hm[1], 10) || 0;
  const candidateDays = getCandidateLocalDays(windowStartLocal, windowEndLocal);

  for (const day of candidateDays) {
    const target = day.set({ hour, minute, second: 0, millisecond: 0 });
    if (target < windowStartLocal || target > windowEndLocal) continue;
    if (isReminderDueToday(reminder, target)) return target;
  }

  return null;
}

app.get('/', function(req, res) {
  res.send('Jordyn Haircare push server is running');
});

app.get('/health', function(req, res) {
  res.json({ ok: true });
});

app.get('/api/push/public-key', function(req, res) {
  res.json({ publicKey: process.env.VAPID_PUBLIC_KEY });
});

async function safeCount(query) {
  try {
    const snap = await query.count().get();
    return snap.data().count || 0;
  } catch (err) {
    console.warn('Count query failed:', err.message);
    return null;
  }
}

function publicUserSummary(docSnap) {
  const user = docSnap.data() || {};
  return {
    uid: user.uid || docSnap.id,
    email: user.email || '',
    displayName: user.displayName || '',
    isAdmin: user.isAdmin === true || ADMIN_EMAILS.has(String(user.email || '').toLowerCase()),
    isPremium: user.isPremium === true,
    subscriptionPlan: user.subscriptionPlan || '',
    notificationsEnabled: user.notificationsEnabled === true,
    updatedAt: user.updatedAt || null
  };
}

app.get('/api/admin/users', async function(req, res) {
  const auth = await requireAdminAuth(req, res);
  if (!auth) return;

  const pageSize = Math.max(20, Math.min(120, parseInt(req.query.limit || '80', 10) || 80));
  const pageToken = String(req.query.pageToken || '').trim();

  try {
    let query = db.collection('users').orderBy(FieldPath.documentId()).limit(pageSize);
    if (pageToken) query = query.startAfter(pageToken);
    const snap = await query.get();
    const users = snap.docs.map(publicUserSummary);
    const lastDoc = snap.docs[snap.docs.length - 1] || null;

    const total = await safeCount(db.collection('users'));
    const premium = await safeCount(db.collection('users').where('isPremium', '==', true));
    const admins = ADMIN_EMAILS.size;

    res.json({
      ok: true,
      users,
      nextPageToken: snap.size === pageSize && lastDoc ? lastDoc.id : '',
      stats: {
        total,
        premium,
        admins,
        trial: total == null || premium == null ? null : Math.max(0, total - premium - admins)
      }
    });
  } catch (err) {
    console.error('Admin users failed:', err.message);
    jsonError(res, 500, 'admin_users_failed', 'Could not load users.');
  }
});

app.post('/api/admin/premium', async function(req, res) {
  const auth = await requireAdminAuth(req, res);
  if (!auth) return;

  const uid = String((req.body || {}).uid || '').trim();
  const grant = (req.body || {}).isPremium === true;
  if (!uid) return jsonError(res, 400, 'missing_uid', 'Missing user id.');

  try {
    await db.collection('users').doc(uid).set({
      isPremium: grant,
      subscriptionPlan: grant ? 'admin_grant' : null,
      updatedAt: FieldValue.serverTimestamp(),
      premiumUpdatedBy: auth.email || auth.uid
    }, { merge: true });
    res.json({ ok: true, uid, isPremium: grant });
  } catch (err) {
    console.error('Admin premium failed:', err.message);
    jsonError(res, 500, 'premium_update_failed', 'Could not update premium.');
  }
});

app.post('/api/admin/broadcast', async function(req, res) {
  const auth = await requireAdminAuth(req, res);
  if (!auth) return;

  const body = sanitizeMessageText((req.body || {}).body, 100);
  const clear = (req.body || {}).clear === true;

  try {
    if (clear) {
      await db.collection('broadcasts').doc('global').set({
        id: '',
        body: '',
        createdBy: '',
        createdByName: '',
        clearedAt: FieldValue.serverTimestamp()
      }, { merge: true });
      lastBroadcastProcessedId = '';
      return res.json({ ok: true, cleared: true });
    }

    if (!body) return jsonError(res, 400, 'missing_message', 'Write a message first.');

    const payload = {
      id: 'bc_' + Date.now(),
      body,
      createdBy: auth.email || auth.uid,
      createdByName: auth.name || auth.email || 'Admin',
      updatedAt: FieldValue.serverTimestamp()
    };
    await db.collection('broadcasts').doc('global').set(payload, { merge: true });
    setImmediate(() => processBroadcasts(true).catch((err) => console.error('broadcast immediate push error', err.message)));
    res.json({ ok: true, broadcast: payload });
  } catch (err) {
    console.error('Admin broadcast failed:', err.message);
    jsonError(res, 500, 'broadcast_failed', 'Broadcast failed.');
  }
});

app.post('/api/promo/redeem', async function(req, res) {
  const auth = await requireFirebaseAuth(req, res);
  if (!auth) return;

  const code = String((req.body || {}).code || '').trim().toUpperCase();
  if (!code) return jsonError(res, 400, 'missing_code', 'Enter a code.');

  try {
    const result = await db.runTransaction(async (tx) => {
      const promoRef = db.collection('promoCodes').doc(code);
      const userRef = db.collection('users').doc(auth.uid);
      const promoSnap = await tx.get(promoRef);
      if (!promoSnap.exists) return { ok: false, code: 'invalid_code', message: 'Invalid code.' };
      const promo = promoSnap.data() || {};
      const used = parseInt(promo.used || 0, 10) || 0;
      const maxUses = parseInt(promo.maxUses || 0, 10) || 0;
      if (maxUses <= 0 || used >= maxUses) return { ok: false, code: 'code_used_up', message: 'Code used up.' };

      tx.set(userRef, {
        isPremium: true,
        subscriptionPlan: 'promo',
        promoCode: code,
        updatedAt: FieldValue.serverTimestamp()
      }, { merge: true });
      tx.update(promoRef, {
        used: used + 1,
        lastUsedAt: FieldValue.serverTimestamp(),
        lastUsedBy: auth.email || auth.uid
      });

      return { ok: true, days: promo.days || 30 };
    });

    if (!result.ok) return jsonError(res, result.code === 'invalid_code' ? 404 : 409, result.code, result.message);
    res.json(result);
  } catch (err) {
    console.error('Promo redeem failed:', err.message);
    jsonError(res, 500, 'promo_failed', 'Could not redeem code.');
  }
});

app.post('/api/push/subscribe', async function(req, res) {
  const auth = await requireFirebaseAuth(req, res);
  if (!auth) return;

  const { email, displayName, timezone, notificationsEnabled, subscription } = req.body || {};
  const uid = auth.uid;

  const cleaned = cleanSubscription(subscription);
  if (!cleaned) return jsonError(res, 400, 'invalid_subscription', 'Missing or invalid push subscription.');
  const userRef = db.collection('users').doc(uid);
  const snap = await userRef.get();
  const existing = snap.exists ? (snap.data() || {}) : {};
  const subscriptions = uniqueByEndpoint([].concat(existing.pushSubscriptions || [], cleaned || []).filter(Boolean));

  await userRef.set({
    uid,
    email: auth.email || email || existing.email || '',
    displayName: auth.name || displayName || existing.displayName || '',
    timezone: getValidTimezone(timezone || existing.timezone),
    notificationsEnabled: notificationsEnabled !== false,
    pushSubscriptions: subscriptions,
    updatedAt: FieldValue.serverTimestamp()
  }, { merge: true });

  await mirrorNotificationPreferenceToUserData(uid, notificationsEnabled !== false, timezone || existing.timezone);

  res.json({ ok: true, count: subscriptions.length });
});

app.post('/api/push/unsubscribe', async function(req, res) {
  const auth = await requireFirebaseAuth(req, res);
  if (!auth) return;

  const { endpoint, notificationsEnabled } = req.body || {};
  if (!endpoint) return jsonError(res, 400, 'missing_endpoint', 'Missing push endpoint.');
  const uid = auth.uid;

  const userRef = db.collection('users').doc(uid);
  const snap = await userRef.get();
  const existing = snap.exists ? (snap.data() || {}) : {};
  const subscriptions = (existing.pushSubscriptions || []).filter((sub) => sub && sub.endpoint !== endpoint);

  await userRef.set({
    notificationsEnabled: !!notificationsEnabled,
    pushSubscriptions: subscriptions,
    updatedAt: FieldValue.serverTimestamp()
  }, { merge: true });

  if (!notificationsEnabled) {
    await mirrorNotificationPreferenceToUserData(uid, false, existing.timezone);
  }

  res.json({ ok: true });
});

app.post('/api/chat', async function(req, res) {
  const auth = await requireFirebaseAuth(req, res);
  if (!auth) return;

  const { message, context, history = [], tier = 'standard' } = req.body || {};
  const safeMessage = sanitizeMessageText(message, 2400);
  if (!safeMessage) return jsonError(res, 400, 'missing_data', 'Missing message.');

  const userRef = db.collection('users').doc(auth.uid);
  const quota = await consumeServerAIQuota(userRef, DateTime.utc(), tier);
  const userDoc = quota.userDoc || {};

  if (!quota.allowed && quota.reason === 'advanced_ai_locked') {
    return jsonError(res, 403, 'advanced_ai_locked', 'Advanced AI is only available for Premium/Admin.', {
      quota: formatQuotaResponse(quota)
    });
  }

  if (!quota.allowed) {
    return jsonError(res, 429, 'quota_exceeded', quota.isAdvanced
      ? 'You have used all 30 Advanced AI messages for today.'
      : 'You have used all 30 free AI messages for this month.', {
      quota: formatQuotaResponse(quota)
    });
  }

  const messages = [{ role: 'system', content: sanitizeMessageText(context || 'You are a warm hair care expert.', 4000) }];
  sanitizeAIHistory(history).forEach((m) => {
    messages.push(m);
  });
  messages.push({ role: 'user', content: safeMessage });

  try {
    const response = await openai.chat.completions.create({
      model: quota.isAdvanced ? 'openrouter/auto' : 'openrouter/free',
      messages
    });
    res.json({
      reply: response.choices[0].message.content,
      quota: formatQuotaResponse(quota),
      tier: quota.isAdvanced ? 'advanced' : 'standard',
      uid: auth.uid,
      plan: userDoc.isAdmin ? 'admin' : userDoc.isPremium ? 'premium' : 'free'
    });
  } catch (err) {
    console.error('AI Error:', err.message);
    await refundServerAIQuota(userRef, quota.periodKey);
    res.status(500).json({ code: 'ai_unavailable', error: 'AI unavailable right now. Try again.' });
  }
});

async function processScheduledNotifications() {
  if (scheduleRunning) return;
  scheduleRunning = true;
  try {
  const usersSnap = await getNextScheduledUserPage();
  const nowUtc = DateTime.utc();
  const windowStartUtc = getCatchupWindowStart(nowUtc, lastScheduleSweepAt);
  lastScheduleSweepAt = nowUtc.toMillis();

  for (const userDocSnap of usersSnap.docs) {
    const userDoc = userDocSnap.data() || {};
    const uid = userDoc.uid || userDocSnap.id;
    let subscriptions = uniqueByEndpoint(userDoc.pushSubscriptions || []);
    if (!subscriptions.length) continue;

    const zone = getValidTimezone(userDoc.timezone);
    const localNow = nowUtc.setZone(zone);
    const localWindowStart = windowStartUtc.setZone(zone);

    const userDataSnap = await db.collection('userData').doc(uid).get();
    const userData = userDataSnap.exists ? (userDataSnap.data() || {}) : {};
    const notificationMeta = pruneNotificationMeta(userDoc.notificationMeta || {}, nowUtc.toMillis());
    let changed = false;

    for (const slot of SMART_NOTIFICATION_SLOTS) {
      const dueMoment = findDueLocalMomentForClock(slot.hour, slot.minute, localWindowStart, localNow);
      if (!dueMoment) continue;
      const logKey = `${dueMoment.toFormat('yyyy-LL-dd')}:${slot.key}`;
      if (notificationMeta[logKey]) continue;

      const payload = buildSmartNotification(slot.key, userDoc, userData);
      subscriptions = await sendToSubscriptions(subscriptions, payload);
      notificationMeta[logKey] = Date.now();
      changed = true;
    }

    const reminders = (userData.reminders || []).filter((item) => item && item.enabled !== false);
    for (const reminder of reminders) {
      const dueMoment = findDueReminderMoment(reminder, localWindowStart, localNow);
      if (!dueMoment) continue;
      const logKey = `${dueMoment.toFormat('yyyy-LL-dd')}:rem:${reminder.id}`;
      if (notificationMeta[logKey]) continue;

      const payload = buildReminderNotification(reminder, userDoc, userData);
      subscriptions = await sendToSubscriptions(subscriptions, payload);
      notificationMeta[logKey] = Date.now();
      changed = true;
    }

    if (changed) {
      await userDocSnap.ref.set({
        pushSubscriptions: subscriptions,
        notificationMeta,
        updatedAt: FieldValue.serverTimestamp()
      }, { merge: true });
    }
  }
  } finally {
    scheduleRunning = false;
  }
}

async function getNextScheduledUserPage() {
  try {
    let query = db.collection('users')
      .where('notificationsEnabled', '==', true)
      .orderBy(FieldPath.documentId())
      .limit(SCHEDULE_SWEEP_LIMIT);
    if (scheduleCursorId) query = query.startAfter(scheduleCursorId);
    let snap = await query.get();
    if (snap.empty && scheduleCursorId) {
      scheduleCursorId = '';
      snap = await db.collection('users')
        .where('notificationsEnabled', '==', true)
        .orderBy(FieldPath.documentId())
        .limit(SCHEDULE_SWEEP_LIMIT)
        .get();
    }
    const last = snap.docs[snap.docs.length - 1];
    scheduleCursorId = snap.size === SCHEDULE_SWEEP_LIMIT && last ? last.id : '';
    return snap;
  } catch (err) {
    console.warn('Paginated schedule query failed, using limited fallback:', err.message);
    scheduleCursorId = '';
    return db.collection('users').where('notificationsEnabled', '==', true).limit(SCHEDULE_SWEEP_LIMIT).get();
  }
}

function pruneNotificationMeta(meta, nowMs) {
  const cutoff = nowMs - (45 * 86400000);
  const next = {};
  Object.keys(meta || {}).forEach((key) => {
    const value = parseInt(meta[key] || 0, 10) || 0;
    if (!value || value >= cutoff) next[key] = meta[key];
  });
  return next;
}

async function processBroadcasts(force) {
  if (broadcastRunning) return;
  broadcastRunning = true;
  try {
    const broadcastSnap = await db.collection('broadcasts').doc('global').get();
    if (!broadcastSnap.exists) return;

    const broadcast = broadcastSnap.data() || {};
    if (!broadcast.id || !broadcast.body) return;
    if (!force && broadcast.id === lastBroadcastProcessedId) return;

    const usersSnap = await db.collection('users').where('notificationsEnabled', '==', true).get();
    for (let i = 0; i < usersSnap.docs.length; i += BROADCAST_BATCH_SIZE) {
      const batch = usersSnap.docs.slice(i, i + BROADCAST_BATCH_SIZE);
      await Promise.all(batch.map(async (userDocSnap) => {
        const userDoc = userDocSnap.data() || {};
        const subscriptions = uniqueByEndpoint(userDoc.pushSubscriptions || []);
        if (!subscriptions.length) return;
        if (userDoc.lastBroadcastPushId === broadcast.id) return;

        const payload = {
          title: '📣 Jordyn update',
          body: String(broadcast.body).slice(0, 100),
          tag: `jhb-broadcast-${broadcast.id}`,
          data: { page: 'Home', url: '/#open-page=Home', broadcastId: broadcast.id }
        };

        const alive = await sendToSubscriptions(subscriptions, payload);
        await userDocSnap.ref.set({
          pushSubscriptions: alive,
          lastBroadcastPushId: broadcast.id,
          updatedAt: FieldValue.serverTimestamp()
        }, { merge: true });
      }));
    }
    lastBroadcastProcessedId = broadcast.id;
  } finally {
    broadcastRunning = false;
  }
}

setInterval(() => {
  processScheduledNotifications().catch((err) => console.error('scheduled notification error', err.message));
}, 60000);

setInterval(() => {
  processBroadcasts().catch((err) => console.error('broadcast push error', err.message));
}, 30000);

const SELF_PING_URL = (process.env.RENDER_EXTERNAL_URL || ('http://localhost:' + PORT)).replace(/\/+$/, '') + '/health';
setInterval(() => {
  const client = SELF_PING_URL.startsWith('https') ? https : http;
  client.get(SELF_PING_URL, () => {}).on('error', () => {});
}, 14 * 60 * 1000);

app.listen(PORT, () => {
  console.log('Server running on ' + PORT);
  processScheduledNotifications().catch((err) => console.error('scheduled notification startup error', err.message));
  processBroadcasts().catch((err) => console.error('broadcast startup error', err.message));
});
