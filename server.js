require('dotenv').config();
const crypto = require('crypto');
const express = require('express');
const cors = require('cors');
const { OpenAI } = require('openai');
const webpush = require('web-push');
const admin = require('firebase-admin');
const { DateTime } = require('luxon');
const Stripe = require('stripe');

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

app.post('/api/stripe/webhook', express.raw({ type: 'application/json' }), handleStripeWebhook);

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
  freePerMonth: 20
};

const MAX_AI_HISTORY_ITEMS = 12;
const STREAK_ACTIVITY_FLAGS = new Set(['CheckIn', 'Growth', 'Gallery', 'wash', 'condition', 'protein', 'hydrate', 'protective', 'oil']);
const AI_REQUEST_TIMEOUT_MS = Math.max(15000, Math.min(60000, parseInt(process.env.AI_REQUEST_TIMEOUT_MS || '35000', 10) || 35000));
const AI_RETRY_DELAY_MS = Math.max(300, Math.min(5000, parseInt(process.env.AI_RETRY_DELAY_MS || '900', 10) || 900));
const SCHEDULE_CATCHUP_MINUTES = Math.max(30, parseInt(process.env.SCHEDULE_CATCHUP_MINUTES || '45', 10) || 45);
const SCHEDULE_SWEEP_LIMIT = Math.max(50, Math.min(300, parseInt(process.env.SCHEDULE_SWEEP_LIMIT || '250', 10) || 250));
const SCHEDULE_SWEEP_INTERVAL_MS = Math.max(60000, Math.min(30 * 60000, parseInt(process.env.SCHEDULE_SWEEP_INTERVAL_MS || '120000', 10) || 120000));
const BROADCAST_POLL_INTERVAL_MS = Math.max(5 * 60000, Math.min(60 * 60000, parseInt(process.env.BROADCAST_POLL_INTERVAL_MS || '900000', 10) || 900000));
const BACKGROUND_JOB_INITIAL_DELAY_MS = Math.max(10000, Math.min(5 * 60000, parseInt(process.env.BACKGROUND_JOB_INITIAL_DELAY_MS || '45000', 10) || 45000));
const FIRESTORE_QUOTA_BACKOFF_MS = Math.max(5 * 60000, Math.min(6 * 60 * 60000, parseInt(process.env.FIRESTORE_QUOTA_BACKOFF_MS || '1800000', 10) || 1800000));
const BROADCAST_BATCH_SIZE = Math.max(10, Math.min(100, parseInt(process.env.BROADCAST_BATCH_SIZE || '40', 10) || 40));
const ADMIN_EMAILS = new Set(['iamkaransingh0709@gmail.com', 'jordynjada03@gmail.com']);
const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY || '';
const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET || '';
const STRIPE_PRICE_PREMIUM_MONTHLY = process.env.STRIPE_PRICE_PREMIUM_MONTHLY || process.env.STRIPE_PREMIUM_PRICE_ID || '';
const STRIPE_PRICE_PREMIUM_YEARLY = process.env.STRIPE_PRICE_PREMIUM_YEARLY || process.env.STRIPE_PREMIUM_YEARLY_PRICE_ID || '';
const STRIPE_CURRENCY = (process.env.STRIPE_CURRENCY || 'usd').toLowerCase();
const STRIPE_PRODUCT_NAME = process.env.STRIPE_PRODUCT_NAME || 'Hair Journal Tracker Premium';
const STRIPE_PREMIUM_MONTHLY_CENTS = Math.max(100, parseInt(process.env.STRIPE_PREMIUM_MONTHLY_CENTS || '1000', 10) || 1000);
const STRIPE_PREMIUM_YEARLY_CENTS = Math.max(100, parseInt(process.env.STRIPE_PREMIUM_YEARLY_CENTS || '9500', 10) || 9500);
const STRIPE_TRIAL_DAYS = Math.max(0, Math.min(60, parseInt(process.env.STRIPE_TRIAL_DAYS || '14', 10) || 14));
const APP_TRIAL_DAYS = Math.max(1, Math.min(60, parseInt(process.env.APP_TRIAL_DAYS || '14', 10) || 14));
const PAYMENT_GRACE_DAYS = Math.max(0, Math.min(14, parseInt(process.env.PAYMENT_GRACE_DAYS || '3', 10) || 3));
const APP_TRIAL_ROLLOUT_ISO = (process.env.APP_TRIAL_ROLLOUT_ISO || '2026-06-11T00:00:00.000Z').trim();
const APP_TRIAL_POPUP_DAYS = new Set([1, 7, 13]);
const ALLOW_STRIPE_TRIAL_CHECKOUT = process.env.ALLOW_STRIPE_TRIAL_CHECKOUT === 'true';
const STRIPE_SUCCESS_URL = (process.env.STRIPE_SUCCESS_URL || 'https://jordyn-haircare.web.app/#open-page=Premium').trim();
const STRIPE_CANCEL_URL = (process.env.STRIPE_CANCEL_URL || 'https://jordyn-haircare.web.app/#open-page=Premium').trim();
const stripe = STRIPE_SECRET_KEY ? new Stripe(STRIPE_SECRET_KEY) : null;

function parseAIModels(value, fallback) {
  const seen = new Set();
  return []
    .concat(fallback || [])
    .concat(String(value || '').split(','))
    .map((model) => String(model || '').trim())
    .filter((model) => {
      if (!model || seen.has(model)) return false;
      seen.add(model);
      return true;
    });
}

const AI_MODELS = {
  advanced: parseAIModels(process.env.AI_ADVANCED_MODELS || process.env.AI_MODEL_ADVANCED, ['openai/gpt-4o-mini', 'google/gemini-2.0-flash-001', 'openrouter/auto']),
  standard: parseAIModels(process.env.AI_STANDARD_MODELS || process.env.AI_MODEL_STANDARD || process.env.AI_MODEL_FREE, ['openai/gpt-4o-mini', 'google/gemini-2.0-flash-001', 'openrouter/auto'])
};

let scheduleCursorId = '';
let scheduleRunning = false;
let broadcastRunning = false;
let lastBroadcastProcessedId = '';
const backgroundJobState = {
  schedule: { blockedUntil: 0, quotaFailures: 0 },
  reminderSchedule: { blockedUntil: 0, quotaFailures: 0 },
  broadcast: { blockedUntil: 0, quotaFailures: 0 }
};
const MAX_PUSH_SUBSCRIPTIONS_PER_USER = Math.max(1, Math.min(5, parseInt(process.env.MAX_PUSH_SUBSCRIPTIONS_PER_USER || '1', 10) || 1));
const MAX_EXACT_REMINDER_DELAY_MS = 7 * 24 * 60 * 60 * 1000;
const registeredReminderTimers = new Map();
const reminderDeliveryLocks = new Set();
let durableReminderSweepRunning = false;

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

function cleanSubscription(subscription, meta) {
  if (!subscription || !subscription.endpoint || !subscription.keys) return null;
  const cleaned = {
    endpoint: subscription.endpoint,
    expirationTime: subscription.expirationTime || null,
    keys: {
      p256dh: subscription.keys.p256dh,
      auth: subscription.keys.auth
    }
  };
  if (meta && meta.clientDeviceId) cleaned.clientDeviceId = String(meta.clientDeviceId).slice(0, 80);
  if (meta && meta.userAgent) cleaned.userAgent = String(meta.userAgent).slice(0, 180);
  cleaned.updatedAtMs = Date.now();
  return cleaned;
}

function getPushDeviceDocId(endpoint) {
  return crypto.createHash('sha256').update(String(endpoint || '')).digest('hex').slice(0, 48);
}

function subscriptionsMatchDevice(left, right) {
  if (!left || !right) return false;
  if (left.endpoint && right.endpoint && left.endpoint === right.endpoint) return true;
  return !!(left.clientDeviceId && right.clientDeviceId && left.clientDeviceId === right.clientDeviceId);
}

async function findLegacyPushOwners(cleaned, currentUid) {
  const owners = [];
  const snap = await db.collection('users').where('notificationsEnabled', '==', true).limit(250).get();
  snap.docs.forEach((docSnap) => {
    if (docSnap.id === currentUid) return;
    const data = docSnap.data() || {};
    if ((data.pushSubscriptions || []).some((sub) => subscriptionsMatchDevice(sub, cleaned))) {
      owners.push(docSnap);
    }
  });
  return owners;
}

async function transferPushDeviceOwnership(uid, cleaned, userPatch) {
  const deviceRef = db.collection('pushDevices').doc(getPushDeviceDocId(cleaned.endpoint));
  const deviceSnap = await deviceRef.get();
  const registeredOwnerUid = deviceSnap.exists ? String((deviceSnap.data() || {}).uid || '') : '';
  let oldOwnerSnaps = [];

  if (registeredOwnerUid && registeredOwnerUid !== uid) {
    const oldSnap = await db.collection('users').doc(registeredOwnerUid).get();
    if (oldSnap.exists) oldOwnerSnaps = [oldSnap];
  } else if (!registeredOwnerUid) {
    oldOwnerSnaps = await findLegacyPushOwners(cleaned, uid);
  }

  const currentRef = db.collection('users').doc(uid);
  const currentSnap = await currentRef.get();
  const current = currentSnap.exists ? (currentSnap.data() || {}) : {};
  const existingSubscriptions = (current.pushSubscriptions || []).filter((sub) => {
    return sub && sub.endpoint && !subscriptionsMatchDevice(sub, cleaned);
  });
  const subscriptions = prunePushSubscriptions([].concat(existingSubscriptions, cleaned).filter(Boolean));
  const batch = db.batch();
  const detachedOwners = [];

  oldOwnerSnaps.forEach((oldSnap) => {
    const oldData = oldSnap.data() || {};
    const remaining = prunePushSubscriptions((oldData.pushSubscriptions || []).filter((sub) => {
      return sub && sub.endpoint && !subscriptionsMatchDevice(sub, cleaned);
    }));
    batch.set(oldSnap.ref, {
      notificationsEnabled: remaining.length > 0,
      pushSubscriptions: remaining,
      pushSubscriptionCount: remaining.length,
      updatedAt: FieldValue.serverTimestamp()
    }, { merge: true });
    detachedOwners.push({
      uid: oldSnap.id,
      enabled: remaining.length > 0,
      timezone: oldData.timezone || ''
    });
  });

  batch.set(currentRef, Object.assign({}, userPatch, {
    uid,
    notificationsEnabled: true,
    pushSubscriptions: subscriptions,
    pushSubscriptionCount: subscriptions.length,
    updatedAt: FieldValue.serverTimestamp()
  }), { merge: true });
  batch.set(deviceRef, {
    uid,
    endpoint: cleaned.endpoint,
    clientDeviceId: cleaned.clientDeviceId || '',
    updatedAt: FieldValue.serverTimestamp()
  }, { merge: true });
  await batch.commit();

  await Promise.all(detachedOwners.map((owner) => {
    return mirrorNotificationPreferenceToUserData(owner.uid, owner.enabled, owner.timezone);
  }));
  return { count: subscriptions.length, detachedUids: detachedOwners.map((owner) => owner.uid) };
}

function getTodayKey(zone) {
  return DateTime.now().setZone(zone || 'UTC').toFormat('yyyy-LL-dd');
}

function isFirestoreQuotaError(err) {
  const code = Number(err && err.code);
  const message = String((err && (err.details || err.message)) || err || '');
  return code === 8 || /RESOURCE_EXHAUSTED|quota exceeded/i.test(message);
}

function canUseScheduleFallback(err) {
  const code = Number(err && err.code);
  const message = String((err && (err.details || err.message)) || err || '');
  return code === 9 || /FAILED_PRECONDITION|requires an index/i.test(message);
}

function recordBackgroundJobFailure(name, err) {
  const state = backgroundJobState[name];
  if (!state || !isFirestoreQuotaError(err)) return false;
  state.quotaFailures += 1;
  const multiplier = Math.pow(2, Math.min(state.quotaFailures - 1, 4));
  const delayMs = Math.min(6 * 60 * 60000, FIRESTORE_QUOTA_BACKOFF_MS * multiplier);
  state.blockedUntil = Date.now() + delayMs;
  console.error(
    `${name} paused for ${Math.round(delayMs / 60000)} minutes after Firestore quota exhaustion:`,
    err && err.message ? err.message : err
  );
  return true;
}

async function runBackgroundJob(name, task) {
  const state = backgroundJobState[name];
  if (!state || Date.now() < state.blockedUntil) return;
  try {
    await task();
    state.quotaFailures = 0;
    state.blockedUntil = 0;
  } catch (err) {
    if (!recordBackgroundJobFailure(name, err)) {
      console.error(`${name} background job error:`, err && err.message ? err.message : err);
    }
  }
}

function startBackgroundLoop(name, intervalMs, task, initialDelayMs) {
  const run = async () => {
    await runBackgroundJob(name, task);
    const nextTimer = setTimeout(run, intervalMs);
    if (typeof nextTimer.unref === 'function') nextTimer.unref();
  };
  const firstTimer = setTimeout(run, initialDelayMs);
  if (typeof firstTimer.unref === 'function') firstTimer.unref();
}

process.on('unhandledRejection', (reason) => {
  if (isFirestoreQuotaError(reason)) {
    console.error(
      'Suppressed unhandled Firestore quota rejection so the server stays healthy:',
      reason && reason.message ? reason.message : reason
    );
    return;
  }
  console.error('Unhandled promise rejection:', reason);
});

function getNotificationName(userDoc, userData) {
  const name = ((userData && userData.profile && userData.profile.name) || userDoc.displayName || 'love').trim();
  return name.split(' ')[0] || 'love';
}

function normalizeServerDotFlags(value) {
  if (Array.isArray(value)) return value.filter(Boolean);
  return value ? [value] : [];
}

function hasServerStreakActivity(userData, dayKey) {
  const dots = (userData && userData.dots) || {};
  const dotActivity = normalizeServerDotFlags(dots[dayKey]).some((flag) => STREAK_ACTIVITY_FLAGS.has(flag));
  if (dotActivity) return true;
  return safeArray(userData && userData.checkins).some((item) => {
    return item && String(item.date || item.ds || '') === dayKey;
  });
}

function calculateServerStreak(userData, localMoment) {
  const maxFreezeDays = 3;
  let cursor = localMoment.startOf('day');
  let anchor = null;
  let blankDaysBeforeAnchor = 0;

  for (let i = 0; i < 365; i += 1) {
    if (hasServerStreakActivity(userData, cursor.toFormat('yyyy-LL-dd'))) {
      anchor = cursor;
      break;
    }
    if (i > 0) blankDaysBeforeAnchor += 1;
    if (blankDaysBeforeAnchor > maxFreezeDays) return 0;
    cursor = cursor.minus({ days: 1 });
  }

  if (!anchor) return 0;
  let streak = 0;
  let gap = 0;
  cursor = anchor;
  for (let i = 0; i < 365; i += 1) {
    if (hasServerStreakActivity(userData, cursor.toFormat('yyyy-LL-dd'))) {
      streak += 1;
      gap = 0;
    } else {
      gap += 1;
      if (gap > maxFreezeDays) break;
    }
    cursor = cursor.minus({ days: 1 });
  }
  return streak;
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
    body: 'Your scheduled hair reminder is ready. Tap to open Hair Journal and stay consistent.',
    tag: `jhb-rem-${reminder.id}`,
    data: { page: 'Calendar', url: '/#open-page=Calendar', reminderId: reminder.id }
  };
}

function getRegisteredReminderKey(uid, reminderId) {
  return `${uid}:${reminderId}`;
}

function getReminderScheduleDocId(uid, reminderId) {
  return crypto.createHash('sha256').update(`${uid}:${reminderId}`).digest('hex').slice(0, 48);
}

function getReminderScheduleRef(uid, reminderId) {
  return db.collection('reminderSchedules').doc(getReminderScheduleDocId(uid, reminderId));
}

function normalizeRegisteredReminder(source) {
  source = source || {};
  return {
    id: sanitizeMessageText(source.id, 100),
    title: sanitizeMessageText(source.title || 'Hair reminder', 120),
    date: sanitizeMessageText(source.date || source.startDate, 20),
    startDate: sanitizeMessageText(source.startDate || source.date, 20),
    type: sanitizeMessageText(source.type || 'custom', 40),
    time: sanitizeMessageText(source.time || '08:00', 8),
    frequency: sanitizeMessageText(source.frequency || 'once', 30),
    enabled: source.enabled !== false
  };
}

function findNextReminderMoment(reminder, zone, fromMoment) {
  const localNow = (fromMoment || DateTime.utc()).setZone(getValidTimezone(zone));
  const hm = String(reminder.time || '08:00').slice(0, 5).split(':');
  const hour = parseInt(hm[0], 10) || 0;
  const minute = parseInt(hm[1], 10) || 0;

  for (let offset = 0; offset <= 370; offset += 1) {
    const target = localNow.startOf('day').plus({ days: offset }).set({ hour, minute, second: 0, millisecond: 0 });
    if (!isReminderDueToday(reminder, target)) continue;
    if (target < localNow.minus({ seconds: 75 })) continue;
    if (target <= localNow.plus({ seconds: 2 })) return localNow.plus({ seconds: 3 });
    return target;
  }
  return null;
}

function cancelRegisteredReminder(uid, reminderId) {
  const key = getRegisteredReminderKey(uid, reminderId);
  const existing = registeredReminderTimers.get(key);
  if (existing && existing.timer) clearTimeout(existing.timer);
  registeredReminderTimers.delete(key);
}

async function cancelDurableReminderSchedule(uid, reminderId) {
  cancelRegisteredReminder(uid, reminderId);
  try {
    await getReminderScheduleRef(uid, reminderId).delete();
  } catch (err) {
    console.warn('Could not delete reminder schedule:', err.message);
  }
}

async function cancelAllRegisteredRemindersForUser(uid) {
  if (!uid) return;
  for (const key of registeredReminderTimers.keys()) {
    if (key.startsWith(`${uid}:`)) {
      const existing = registeredReminderTimers.get(key);
      if (existing && existing.timer) clearTimeout(existing.timer);
      registeredReminderTimers.delete(key);
    }
  }

  try {
    const snap = await db.collection('reminderSchedules').where('uid', '==', uid).limit(100).get();
    if (snap.empty) return;
    const batch = db.batch();
    snap.docs.forEach((docSnap) => batch.delete(docSnap.ref));
    await batch.commit();
  } catch (err) {
    console.warn('Could not clear reminder schedules for user:', err.message);
  }
}

async function persistRegisteredReminderSchedule(uid, reminder, zone, dueMoment) {
  const clean = normalizeRegisteredReminder(reminder);
  if (!uid || !clean.id || !dueMoment || !dueMoment.isValid) return false;
  await getReminderScheduleRef(uid, clean.id).set({
    uid,
    reminderId: clean.id,
    reminder: clean,
    timezone: getValidTimezone(zone),
    dueAtMs: dueMoment.toUTC().toMillis(),
    dueIso: dueMoment.toISO(),
    updatedAt: FieldValue.serverTimestamp()
  }, { merge: true });
  return true;
}

async function reconcileRegisteredReminderSchedules(uid, reminders, zone) {
  const cleanReminders = safeArray(reminders)
    .slice(0, 50)
    .map(normalizeRegisteredReminder)
    .filter((reminder) => reminder.id && reminder.enabled !== false);
  const desiredIds = new Set(cleanReminders.map((reminder) => reminder.id));
  const existingSnap = await db.collection('reminderSchedules').where('uid', '==', uid).limit(200).get();
  const staleDocs = existingSnap.docs.filter((docSnap) => {
    const data = docSnap.data() || {};
    return !desiredIds.has(String(data.reminderId || ''));
  });

  if (staleDocs.length) {
    const batch = db.batch();
    staleDocs.forEach((docSnap) => {
      const data = docSnap.data() || {};
      if (data.reminderId) cancelRegisteredReminder(uid, data.reminderId);
      batch.delete(docSnap.ref);
    });
    await batch.commit();
  }

  let scheduled = 0;
  for (const reminder of cleanReminders) {
    const dueMoment = findNextReminderMoment(reminder, zone, DateTime.utc());
    if (!dueMoment) {
      await cancelDurableReminderSchedule(uid, reminder.id);
      continue;
    }
    armRegisteredReminder(uid, reminder, zone, DateTime.utc());
    await persistRegisteredReminderSchedule(uid, reminder, zone, dueMoment);
    scheduled += 1;
  }

  return { scheduled, removed: staleDocs.length };
}

async function advanceRegisteredReminderSchedule(uid, reminder, zone, dueMoment) {
  const nextDue = findNextReminderMoment(reminder, zone, dueMoment.plus({ minutes: 1 }));
  if (!nextDue) {
    await cancelDurableReminderSchedule(uid, reminder.id);
    return null;
  }
  armRegisteredReminder(uid, reminder, zone, dueMoment.plus({ minutes: 1 }));
  await persistRegisteredReminderSchedule(uid, reminder, zone, nextDue);
  return nextDue;
}

async function deferRegisteredReminderSchedule(uid, reminderId, delayMs, message) {
  try {
    await getReminderScheduleRef(uid, reminderId).set({
      dueAtMs: Date.now() + Math.max(60000, delayMs || 300000),
      lastError: sanitizeMessageText(message || 'Delivery deferred', 180),
      lastDeferredAt: FieldValue.serverTimestamp(),
      updatedAt: FieldValue.serverTimestamp()
    }, { merge: true });
  } catch (err) {
    console.warn('Could not defer reminder schedule:', err.message);
  }
}

async function deliverRegisteredReminder(uid, reminderId, dueIso, scheduledReminder, scheduledZone) {
  const key = getRegisteredReminderKey(uid, reminderId);
  registeredReminderTimers.delete(key);
  if (reminderDeliveryLocks.has(key)) return false;
  reminderDeliveryLocks.add(key);

  try {
    const userRef = db.collection('users').doc(uid);
    const scheduleRef = getReminderScheduleRef(uid, reminderId);
    const [userSnap, dataSnap, scheduleSnap] = await Promise.all([
      userRef.get(),
      db.collection('userData').doc(uid).get(),
      scheduleRef.get()
    ]);
    if (!userSnap.exists) {
      await cancelDurableReminderSchedule(uid, reminderId);
      return false;
    }
    const userDoc = userSnap.data() || {};
    const userData = dataSnap.exists ? (dataSnap.data() || {}) : {};
    const scheduleData = scheduleSnap.exists ? (scheduleSnap.data() || {}) : {};
    if (!hasPremiumAccess(userDoc)) {
      await cancelDurableReminderSchedule(uid, reminderId);
      return false;
    }
    if (userDoc.notificationsEnabled !== true) {
      await deferRegisteredReminderSchedule(uid, reminderId, 30 * 60000, 'Notifications are disabled');
      return false;
    }
    let subscriptions = prunePushSubscriptions(userDoc.pushSubscriptions || []);
    if (!subscriptions.length) {
      await deferRegisteredReminderSchedule(uid, reminderId, 10 * 60000, 'No active push subscription');
      return false;
    }

    const cloudReminder = safeArray(userData.reminders).find((item) => item && String(item.id) === String(reminderId));
    const reminder = normalizeRegisteredReminder(scheduleData.reminder || cloudReminder || scheduledReminder);
    if (!reminder.id || reminder.enabled === false) {
      await cancelDurableReminderSchedule(uid, reminderId);
      return false;
    }
    const zone = getValidTimezone(scheduleData.timezone || scheduledZone || userDoc.timezone || (userData.profile && userData.profile.timezone));
    const dueMoment = DateTime.fromISO(scheduleData.dueIso || dueIso, { setZone: true });
    if (!dueMoment.isValid) {
      await cancelDurableReminderSchedule(uid, reminderId);
      return false;
    }
    const logKey = `${dueMoment.toFormat('yyyy-LL-dd')}:rem:${reminder.id}`;
    const notificationMeta = pruneNotificationMeta(userDoc.notificationMeta || {}, Date.now());
    if (notificationMeta[logKey]) {
      await advanceRegisteredReminderSchedule(uid, reminder, zone, dueMoment);
      return false;
    }

    subscriptions = await sendToSubscriptions(subscriptions, buildReminderNotification(reminder, userDoc, userData));
    notificationMeta[logKey] = Date.now();
    await userRef.set({
      pushSubscriptions: subscriptions,
      pushSubscriptionCount: subscriptions.length,
      notificationMeta,
      updatedAt: FieldValue.serverTimestamp()
    }, { merge: true });

    await advanceRegisteredReminderSchedule(uid, reminder, zone, dueMoment);
    return true;
  } catch (err) {
    console.error('Exact reminder delivery failed:', err.message);
    await deferRegisteredReminderSchedule(uid, reminderId, 5 * 60000, err.message);
    return false;
  } finally {
    reminderDeliveryLocks.delete(key);
  }
}

function armRegisteredReminder(uid, reminder, zone, fromMoment) {
  if (!uid || !reminder || !reminder.id || reminder.enabled === false) {
    if (uid && reminder && reminder.id) cancelRegisteredReminder(uid, reminder.id);
    return null;
  }
  const dueMoment = findNextReminderMoment(reminder, zone, fromMoment || DateTime.utc());
  if (!dueMoment) {
    cancelRegisteredReminder(uid, reminder.id);
    return null;
  }
  const delayMs = dueMoment.toUTC().toMillis() - Date.now();
  if (delayMs <= 0 || delayMs > MAX_EXACT_REMINDER_DELAY_MS) return dueMoment;

  const key = getRegisteredReminderKey(uid, reminder.id);
  const current = registeredReminderTimers.get(key);
  const dueIso = dueMoment.toISO();
  if (current && current.dueIso === dueIso) return dueMoment;
  if (current && current.timer) clearTimeout(current.timer);

  const timer = setTimeout(() => {
    deliverRegisteredReminder(uid, reminder.id, dueIso, reminder, zone).catch((err) => {
      console.error('Registered reminder timer failed:', err.message);
    });
  }, Math.max(250, delayMs));
  if (typeof timer.unref === 'function') timer.unref();
  registeredReminderTimers.set(key, { timer, dueIso, reminder, zone });
  return dueMoment;
}

async function processDurableReminderSchedules() {
  if (durableReminderSweepRunning) return { running: true };
  durableReminderSweepRunning = true;
  const stats = { checked: 0, delivered: 0 };
  try {
    const snap = await db.collection('reminderSchedules')
      .where('dueAtMs', '<=', Date.now() + 2000)
      .limit(100)
      .get();
    for (const docSnap of snap.docs) {
      const data = docSnap.data() || {};
      stats.checked += 1;
      if (!data.uid || !data.reminderId) {
        await docSnap.ref.delete();
        continue;
      }
      const delivered = await deliverRegisteredReminder(
        data.uid,
        data.reminderId,
        data.dueIso || DateTime.fromMillis(data.dueAtMs || Date.now()).toISO(),
        data.reminder,
        data.timezone
      );
      if (delivered) stats.delivered += 1;
    }
    return stats;
  } finally {
    durableReminderSweepRunning = false;
  }
}

function buildSmartNotification(slotKey, userDoc, userData, localMoment) {
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
  const notificationMoment = localMoment || DateTime.utc();
  const todayKey = notificationMoment.toFormat('yyyy-LL-dd');
  const streak = calculateServerStreak(userData || {}, notificationMoment);
  if (!hasServerStreakActivity(userData || {}, todayKey) && streak > 0) {
    return {
      title: `🔥 ${name}, save your streak today`,
      body: `Your ${streak}-day streak is waiting for one small check-in. Open Hair Journal before the day ends.`,
      tag: 'jhb-streak-save',
      data: { page: 'Checkin', url: '/#open-page=Checkin' }
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
  const targets = prunePushSubscriptions(subscriptions || []);
  for (const sub of targets) {
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

async function sendToSubscriptionsWithStats(subscriptions, payload) {
  const alive = [];
  const stats = {
    sent: 0,
    failed: 0,
    removed: 0
  };

  const targets = prunePushSubscriptions(subscriptions || []);
  for (const sub of targets) {
    try {
      await webpush.sendNotification(sub, JSON.stringify(payload));
      stats.sent += 1;
      alive.push(sub);
    } catch (err) {
      const code = err && err.statusCode;
      if (code === 404 || code === 410) {
        stats.removed += 1;
      } else {
        stats.failed += 1;
        alive.push(sub);
        console.warn('Broadcast push failed for one subscription:', (err && err.message) || err);
      }
    }
  }

  return { alive, stats };
}

function isAlreadyExistsError(err) {
  const code = String((err && err.code) || '').toLowerCase();
  const message = String((err && err.message) || '').toLowerCase();
  return code === '6' || code === 'already-exists' || message.includes('already exists');
}

function getBroadcastDeliveryRef(broadcastId, endpoint) {
  const id = crypto.createHash('sha256').update(`${broadcastId}:${endpoint}`).digest('hex').slice(0, 48);
  return db.collection('broadcastDeliveries').doc(id);
}

async function claimBroadcastDelivery(broadcastId, endpoint) {
  const ref = getBroadcastDeliveryRef(broadcastId, endpoint);
  try {
    await ref.create({
      broadcastId,
      endpointHash: getPushDeviceDocId(endpoint),
      claimedAt: FieldValue.serverTimestamp()
    });
    return { claimed: true, ref };
  } catch (err) {
    if (isAlreadyExistsError(err)) return { claimed: false, ref };
    throw err;
  }
}

async function sendBroadcastToSubscriptionsOnce(broadcastId, subscriptions, payload) {
  const alive = [];
  const stats = {
    sent: 0,
    failed: 0,
    removed: 0,
    skippedDuplicate: 0
  };

  const targets = prunePushSubscriptions(subscriptions || []);
  for (const sub of targets) {
    const claim = await claimBroadcastDelivery(broadcastId, sub.endpoint);
    if (!claim.claimed) {
      stats.skippedDuplicate += 1;
      alive.push(sub);
      continue;
    }

    try {
      await webpush.sendNotification(sub, JSON.stringify(payload));
      stats.sent += 1;
      alive.push(sub);
    } catch (err) {
      const code = err && err.statusCode;
      if (code === 404 || code === 410) {
        stats.removed += 1;
      } else {
        stats.failed += 1;
        alive.push(sub);
        await claim.ref.delete().catch(() => null);
        console.warn('Broadcast push failed for one subscription:', (err && err.message) || err);
      }
    }
  }

  return { alive, stats };
}

function uniqueByEndpoint(list) {
  const seen = new Set();
  return (list || []).filter((sub) => {
    if (!sub || !sub.endpoint || seen.has(sub.endpoint)) return false;
    seen.add(sub.endpoint);
    return true;
  });
}

function getPushSubscriptionCount(userDoc) {
  const stored = Number((userDoc || {}).pushSubscriptionCount || 0) || 0;
  const live = uniqueByEndpoint((userDoc || {}).pushSubscriptions || []).length;
  return Math.max(stored, live);
}

function prunePushSubscriptions(list) {
  const unique = uniqueByEndpoint(list || []);
  unique.sort((a, b) => Number(a.updatedAtMs || 0) - Number(b.updatedAtMs || 0));
  return unique.slice(Math.max(0, unique.length - MAX_PUSH_SUBSCRIPTIONS_PER_USER));
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
    pushLinked: !!enabled
  };
  if (timezone) profilePatch.timezone = getValidTimezone(timezone);
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

function timestampToMillis(value) {
  if (!value) return 0;
  if (typeof value.toMillis === 'function') return value.toMillis();
  if (typeof value.toDate === 'function') return value.toDate().getTime();
  if (value instanceof Date) return value.getTime();
  if (typeof value === 'number') return value < 10000000000 ? value * 1000 : value;
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? parsed : 0;
}

function dateFromMillis(ms) {
  const value = Number(ms || 0);
  return value > 0 ? new Date(value) : null;
}

function isoFromMillis(ms) {
  const date = dateFromMillis(ms);
  return date ? date.toISOString() : null;
}

function getTrialRolloutMillis() {
  const parsed = Date.parse(APP_TRIAL_ROLLOUT_ISO);
  return Number.isFinite(parsed) ? parsed : Date.UTC(2026, 5, 11);
}

function normalizeSubscriptionStatus(value) {
  const status = String(value || '').toLowerCase().replace(/\s+/g, '_').trim();
  if (status === 'payment_failed') return 'past_due';
  if (status === 'paid' || status === 'premium') return 'active';
  return status;
}

function getSubscriptionStatus(userDoc) {
  return normalizeSubscriptionStatus(
    (userDoc && (userDoc.subscription_status || userDoc.subscriptionStatus || userDoc.premiumStatus)) || ''
  );
}

function buildAccessState(userDoc, nowUtc) {
  userDoc = userDoc || {};
  const nowMs = nowUtc && typeof nowUtc.toMillis === 'function' ? nowUtc.toMillis() : Date.now();
  const status = getSubscriptionStatus(userDoc);
  const isAdmin = userDoc.isAdmin === true || ADMIN_EMAILS.has(String(userDoc.email || '').toLowerCase());
  const subscriptionPlan = String(userDoc.subscriptionPlan || '').toLowerCase();
  const trialStartMs = timestampToMillis(userDoc.trial_start_date || userDoc.trialStartDate);
  const trialEndMs = timestampToMillis(userDoc.trial_end_date || userDoc.trialEndDate)
    || (trialStartMs ? trialStartMs + APP_TRIAL_DAYS * 86400000 : 0);
  const currentPeriodEndMs = timestampToMillis(userDoc.subscription_current_period_end || userDoc.subscriptionCurrentPeriodEnd || userDoc.stripeCurrentPeriodEnd);
  const storedGraceUntilMs = timestampToMillis(userDoc.subscription_grace_until || userDoc.subscriptionGraceUntil);
  const paymentProblem = userDoc.premiumPaymentProblem === true || status === 'past_due' || status === 'unpaid';
  const derivedGraceUntilMs = paymentProblem && !storedGraceUntilMs && currentPeriodEndMs
    ? currentPeriodEndMs + PAYMENT_GRACE_DAYS * 86400000
    : 0;
  const graceUntilMs = Math.max(storedGraceUntilMs || 0, derivedGraceUntilMs || 0);
  const trialActive = !!(trialStartMs && trialEndMs && nowMs < trialEndMs);
  const manualPremium = userDoc.isPremium === true && subscriptionPlan && subscriptionPlan !== 'stripe';
  const stripeStatusActive = !paymentProblem && (status === 'active' || status === 'trialing');
  const legacyStripeActive = userDoc.isPremium === true
    && subscriptionPlan === 'stripe'
    && !paymentProblem
    && (!currentPeriodEndMs || nowMs <= currentPeriodEndMs + 3600000);
  const subscriptionActive = manualPremium || stripeStatusActive || legacyStripeActive;
  const graceActive = !!(paymentProblem && graceUntilMs && nowMs < graceUntilMs);

  let state = 'expired';
  let allowed = false;
  let reason = 'trial_expired';
  if (isAdmin) {
    allowed = true;
    state = 'admin';
    reason = 'admin';
  } else if (subscriptionActive) {
    allowed = true;
    state = 'subscribed';
    reason = 'subscription_active';
  } else if (graceActive) {
    allowed = true;
    state = 'grace';
    reason = 'payment_grace';
  } else if (trialActive) {
    allowed = true;
    state = 'trial';
    reason = 'trial_active';
  } else if (paymentProblem) {
    state = 'payment_failed';
    reason = 'payment_failed';
  }

  return {
    verified: true,
    allowed,
    state,
    reason,
    isAdmin,
    subscriptionStatus: status || (subscriptionActive ? 'active' : 'none'),
    subscriptionPlan: subscriptionPlan || null,
    paymentProblem,
    trialActive,
    trialStartMs,
    trialEndMs,
    trialDaysLeft: trialEndMs ? Math.max(0, Math.ceil((trialEndMs - nowMs) / 86400000)) : 0,
    graceActive,
    graceUntilMs,
    currentPeriodEndMs,
    serverNowMs: nowMs
  };
}

function serializeAccessState(access) {
  access = access || {};
  return {
    verified: access.verified === true,
    allowed: access.allowed === true,
    state: access.state || 'unknown',
    reason: access.reason || '',
    isAdmin: access.isAdmin === true,
    subscriptionStatus: access.subscriptionStatus || 'none',
    subscriptionPlan: access.subscriptionPlan || null,
    paymentProblem: access.paymentProblem === true,
    trialActive: access.trialActive === true,
    trialStartDate: isoFromMillis(access.trialStartMs),
    trialEndDate: isoFromMillis(access.trialEndMs),
    trialDaysLeft: Math.max(0, parseInt(access.trialDaysLeft || 0, 10) || 0),
    graceActive: access.graceActive === true,
    graceUntilDate: isoFromMillis(access.graceUntilMs),
    currentPeriodEndDate: isoFromMillis(access.currentPeriodEndMs),
    monthlyPriceCents: STRIPE_PREMIUM_MONTHLY_CENTS,
    monthlyPriceLabel: '$10/month',
    serverNow: isoFromMillis(access.serverNowMs || Date.now())
  };
}

function buildTrialPopup(access, userDoc, timezone) {
  if (!access || access.state !== 'trial' || !access.trialStartMs || !access.trialEndMs) return null;
  const zone = getValidTimezone(timezone || userDoc.timezone);
  const nowLocal = DateTime.fromMillis(access.serverNowMs || Date.now(), { zone: 'utc' }).setZone(zone).startOf('day');
  const startLocal = DateTime.fromMillis(access.trialStartMs, { zone: 'utc' }).setZone(zone).startOf('day');
  const day = Math.floor(nowLocal.diff(startLocal, 'days').days) + 1;
  if (!APP_TRIAL_POPUP_DAYS.has(day)) return null;
  const seen = userDoc.trialPopupSeenDays || {};
  if (seen[String(day)] || (Array.isArray(seen) && seen.indexOf(day) !== -1)) return null;

  const copyByDay = {
    1: 'You have 14 days of free access — no credit card needed. Enjoy everything!',
    7: '7 days left on your free trial — upgrade to keep access for just $10/month',
    13: 'Your free trial ends tomorrow — subscribe for $10/month to keep using the app'
  };

  return {
    day,
    title: day === 1 ? 'Welcome to your free trial' : day === 7 ? 'Halfway through your trial' : 'Trial ends tomorrow',
    message: copyByDay[day],
    ctaLabel: 'Upgrade Now — $10/month',
    dismissLabel: day === 13 ? 'I understand' : 'Remind Me Later',
    forceAcknowledge: day === 13,
    trialEndDate: isoFromMillis(access.trialEndMs),
    monthlyPriceLabel: '$10/month'
  };
}

async function ensureTrialAndAccessState(auth, options) {
  options = options || {};
  const userRef = db.collection('users').doc(auth.uid);
  const nowMs = Date.now();
  const requestedZone = options.timezone ? getValidTimezone(options.timezone) : '';
  const result = await db.runTransaction(async (tx) => {
    const snap = await tx.get(userRef);
    const existing = snap.exists ? (snap.data() || {}) : {};
    const zone = requestedZone || getValidTimezone(existing.timezone || 'UTC');
    const materialized = Object.assign({
      uid: auth.uid,
      email: auth.email || existing.email || '',
      displayName: auth.name || existing.displayName || '',
      isAdmin: existing.isAdmin === true || ADMIN_EMAILS.has(String(auth.email || existing.email || '').toLowerCase()),
      isPremium: existing.isPremium === true,
      subscriptionPlan: existing.subscriptionPlan || null
    }, existing);
    const patch = {
      uid: auth.uid,
      email: auth.email || existing.email || '',
      displayName: auth.name || existing.displayName || '',
      updatedAt: FieldValue.serverTimestamp()
    };
    const localPatch = { timezone: zone };
    if (!snap.exists && !existing.createdAt) patch.createdAt = FieldValue.serverTimestamp();
    if (requestedZone || !existing.timezone) patch.timezone = zone;

    const existingTrialStartMs = timestampToMillis(existing.trial_start_date || existing.trialStartDate);
    if (!existingTrialStartMs) {
      const createdMs = timestampToMillis(existing.createdAt);
      const rolloutMs = getTrialRolloutMillis();
      const startMs = snap.exists ? Math.max(createdMs || rolloutMs, rolloutMs) : Math.max(nowMs, rolloutMs);
      patch.trial_start_date = dateFromMillis(startMs);
      patch.trial_end_date = dateFromMillis(startMs + APP_TRIAL_DAYS * 86400000);
      localPatch.trial_start_date = patch.trial_start_date;
      localPatch.trial_end_date = patch.trial_end_date;
    } else if (!timestampToMillis(existing.trial_end_date || existing.trialEndDate)) {
      patch.trial_end_date = dateFromMillis(existingTrialStartMs + APP_TRIAL_DAYS * 86400000);
      localPatch.trial_end_date = patch.trial_end_date;
    }

    if (!existing.subscription_status && !existing.subscriptionStatus) {
      if (existing.isPremium === true && !existing.premiumPaymentProblem) {
        patch.subscription_status = 'active';
        localPatch.subscription_status = 'active';
      } else if (existing.premiumPaymentProblem === true) {
        patch.subscription_status = 'past_due';
        localPatch.subscription_status = 'past_due';
      }
    }

    tx.set(userRef, compactDefined(patch), { merge: true });
    return Object.assign({}, materialized, localPatch);
  });

  const access = buildAccessState(result, DateTime.fromMillis(nowMs, { zone: 'utc' }));
  const popupZone = requestedZone || result.timezone || 'UTC';
  return {
    userRef,
    userDoc: result,
    access,
    popup: buildTrialPopup(access, result, popupZone)
  };
}

async function requireActiveAppAccess(req, res, auth) {
  try {
    const body = req.body || {};
    const guard = await ensureTrialAndAccessState(auth, {
      timezone: body.timezone || req.query.timezone || ''
    });
    if (!guard.access.allowed) {
      jsonError(res, 402, 'access_required', 'Your free trial has ended. Subscribe for $10/month to keep using Hair Journal.', {
        access: serializeAccessState(guard.access)
      });
      return null;
    }
    return guard;
  } catch (err) {
    console.error('Access check failed:', err.message);
    jsonError(res, 500, 'access_check_failed', 'Could not verify app access. Please try again.');
    return null;
  }
}

function hasPremiumAccess(userDoc) {
  return buildAccessState(userDoc, DateTime.utc()).allowed === true;
}

function sanitizeMessageText(value, maxLength) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, maxLength);
}

function sanitizeAIReplyText(value, maxLength) {
  return String(value || '')
    .replace(/\r\n?/g, '\n')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{4,}/g, '\n\n\n')
    .trim()
    .slice(0, maxLength);
}

function compactDefined(value) {
  const out = {};
  Object.keys(value || {}).forEach((key) => {
    if (value[key] !== undefined) out[key] = value[key];
  });
  return out;
}

function stripeUnixToDate(value) {
  const n = parseInt(value || 0, 10) || 0;
  return n > 0 ? new Date(n * 1000) : null;
}

function stripeReady(res) {
  if (!stripe) {
    jsonError(res, 503, 'stripe_not_configured', 'Stripe is not configured yet.');
    return false;
  }
  return true;
}

function getStripeLineItemForPlan(plan) {
  const isYearly = plan === 'yearly';
  const configuredPrice = isYearly ? STRIPE_PRICE_PREMIUM_YEARLY : STRIPE_PRICE_PREMIUM_MONTHLY;
  if (configuredPrice) return { price: configuredPrice, quantity: 1 };

  return {
    quantity: 1,
    price_data: {
      currency: STRIPE_CURRENCY,
      unit_amount: isYearly ? STRIPE_PREMIUM_YEARLY_CENTS : STRIPE_PREMIUM_MONTHLY_CENTS,
      recurring: { interval: isYearly ? 'year' : 'month' },
      product_data: {
        name: STRIPE_PRODUCT_NAME,
        description: isYearly
          ? 'Annual premium hair journal subscription'
          : 'Monthly premium hair journal subscription'
      }
    }
  };
}

function addQueryParam(url, key, value) {
  const hashIndex = url.indexOf('#');
  const base = hashIndex === -1 ? url : url.slice(0, hashIndex);
  const hash = hashIndex === -1 ? '' : url.slice(hashIndex);
  const joiner = base.indexOf('?') === -1 ? '?' : '&';
  const encodedValue = String(value) === '{CHECKOUT_SESSION_ID}' ? value : encodeURIComponent(value);
  return base + joiner + encodeURIComponent(key) + '=' + encodedValue + hash;
}

async function findUserRefByStripeCustomer(customerId) {
  if (!customerId) return null;
  const snap = await db.collection('users').where('stripeCustomerId', '==', customerId).limit(1).get();
  return snap.empty ? null : snap.docs[0].ref;
}

async function ensureStripeCustomer(auth, userRef, userDoc) {
  if (userDoc && userDoc.stripeCustomerId) return userDoc.stripeCustomerId;

  const customer = await stripe.customers.create(compactDefined({
    email: auth.email || userDoc.email || undefined,
    name: auth.name || userDoc.displayName || undefined,
    metadata: {
      uid: auth.uid,
      app: 'jordyn-haircare'
    }
  }));

  await userRef.set({
    stripeCustomerId: customer.id,
    updatedAt: FieldValue.serverTimestamp()
  }, { merge: true });

  return customer.id;
}

async function writeStripePremiumState(uid, state) {
  const userRef = db.collection('users').doc(uid);
  const requestedStatus = normalizeSubscriptionStatus(state.subscriptionStatus || state.status || (state.isPremium ? 'active' : 'inactive'));
  const paymentProblem = state.paymentProblem === true || requestedStatus === 'past_due' || requestedStatus === 'unpaid';
  const isPremium = state.isPremium === true && !paymentProblem;

  await db.runTransaction(async (tx) => {
    const snap = await tx.get(userRef);
    const existing = snap.exists ? (snap.data() || {}) : {};

    if (!isPremium && existing.isAdmin === true) return;
    if (!isPremium && existing.subscriptionPlan && existing.subscriptionPlan !== 'stripe') return;
    if (!isPremium && state.stripeSubscriptionId && existing.stripeSubscriptionId && existing.stripeSubscriptionId !== state.stripeSubscriptionId) return;

    const nowMs = Date.now();
    const existingGraceUntilMs = timestampToMillis(existing.subscription_grace_until || existing.subscriptionGraceUntil);
    const nextGraceUntilMs = paymentProblem
      ? (existingGraceUntilMs > nowMs ? existingGraceUntilMs : nowMs + PAYMENT_GRACE_DAYS * 86400000)
      : 0;
    const subscriptionStatus = paymentProblem ? 'past_due' : (isPremium ? 'active' : (requestedStatus || 'inactive'));

    tx.set(userRef, compactDefined({
      isPremium,
      subscriptionPlan: isPremium || paymentProblem ? 'stripe' : null,
      premiumSource: isPremium ? 'stripe' : null,
      premiumStatus: requestedStatus || null,
      subscription_status: subscriptionStatus,
      subscription_grace_until: nextGraceUntilMs ? dateFromMillis(nextGraceUntilMs) : null,
      subscription_current_period_end: state.currentPeriodEnd || null,
      stripeCustomerId: state.stripeCustomerId || existing.stripeCustomerId || undefined,
      stripeSubscriptionId: state.stripeSubscriptionId || existing.stripeSubscriptionId || null,
      stripePriceId: state.stripePriceId || existing.stripePriceId || null,
      stripeCancelAtPeriodEnd: state.cancelAtPeriodEnd === undefined ? null : !!state.cancelAtPeriodEnd,
      stripeCurrentPeriodEnd: state.currentPeriodEnd || null,
      stripeTrialEnd: state.trialEnd || null,
      premiumPaymentProblem: paymentProblem,
      updatedAt: FieldValue.serverTimestamp()
    }), { merge: true });
  });

  await db.collection('userData').doc(uid).set({
    profile: compactDefined({
      premium: isPremium,
      isPremium,
      subscriptionPlan: isPremium ? 'stripe' : null,
      premiumStatus: requestedStatus || null,
      subscriptionStatus: paymentProblem ? 'past_due' : (isPremium ? 'active' : (requestedStatus || 'inactive')),
      stripeCurrentPeriodEnd: state.currentPeriodEnd || null
    }),
    syncMeta: {
      collections: {
        profile: Date.now()
      }
    },
    updatedAt: FieldValue.serverTimestamp()
  }, { merge: true });
}

async function syncStripeSubscription(subscription, fallbackUid) {
  if (!subscription) return null;
  const customerId = typeof subscription.customer === 'string' ? subscription.customer : subscription.customer && subscription.customer.id;
  const uidFromMeta = subscription.metadata && subscription.metadata.uid;
  let uid = fallbackUid || uidFromMeta || '';
  let userRef = uid ? db.collection('users').doc(uid) : null;
  if (!userRef && customerId) userRef = await findUserRefByStripeCustomer(customerId);
  if (!userRef) return null;
  uid = userRef.id;

  const status = String(subscription.status || '').toLowerCase();
  const paidStatuses = new Set(['active', 'trialing']);
  const firstItem = subscription.items && subscription.items.data && subscription.items.data[0];
  const priceId = firstItem && firstItem.price && firstItem.price.id;

  await writeStripePremiumState(uid, {
    isPremium: paidStatuses.has(status),
    status,
    stripeCustomerId: customerId,
    stripeSubscriptionId: subscription.id,
    stripePriceId: priceId,
    cancelAtPeriodEnd: subscription.cancel_at_period_end,
    currentPeriodEnd: stripeUnixToDate(subscription.current_period_end),
    trialEnd: stripeUnixToDate(subscription.trial_end),
    paymentProblem: status === 'past_due'
  });

  return uid;
}

async function handleStripeWebhook(req, res) {
  if (!stripe || !STRIPE_WEBHOOK_SECRET) {
    console.error('Stripe webhook hit before Stripe env was configured.');
    return res.status(503).send('stripe_not_configured');
  }

  let event;
  try {
    event = stripe.webhooks.constructEvent(req.body, req.headers['stripe-signature'], STRIPE_WEBHOOK_SECRET);
  } catch (err) {
    console.error('Stripe webhook signature failed:', err.message);
    return res.status(400).send('bad_signature');
  }

  try {
    if (event.type === 'checkout.session.completed') {
      const session = event.data.object || {};
      const uid = (session.metadata && session.metadata.uid) || session.client_reference_id || '';
      if (session.subscription) {
        const subscription = typeof session.subscription === 'string'
          ? await stripe.subscriptions.retrieve(session.subscription)
          : session.subscription;
        await syncStripeSubscription(subscription, uid);
      } else if (uid) {
        await writeStripePremiumState(uid, {
          isPremium: true,
          status: 'active',
          stripeCustomerId: typeof session.customer === 'string' ? session.customer : session.customer && session.customer.id
        });
      }
    } else if (event.type === 'customer.subscription.created' || event.type === 'customer.subscription.updated') {
      await syncStripeSubscription(event.data.object);
    } else if (event.type === 'customer.subscription.deleted') {
      const subscription = event.data.object || {};
      const customerId = typeof subscription.customer === 'string' ? subscription.customer : subscription.customer && subscription.customer.id;
      const uid = subscription.metadata && subscription.metadata.uid;
      const userRef = uid ? db.collection('users').doc(uid) : await findUserRefByStripeCustomer(customerId);
      if (userRef) {
        await writeStripePremiumState(userRef.id, {
          isPremium: false,
          status: 'canceled',
          stripeCustomerId: customerId,
          stripeSubscriptionId: subscription.id
        });
      }
    } else if (event.type === 'invoice.payment_succeeded' || event.type === 'invoice.payment_failed') {
      const invoice = event.data.object || {};
      const customerId = typeof invoice.customer === 'string' ? invoice.customer : invoice.customer && invoice.customer.id;
      const subscriptionId = typeof invoice.subscription === 'string' ? invoice.subscription : invoice.subscription && invoice.subscription.id;
      if (subscriptionId) {
        const subscription = await stripe.subscriptions.retrieve(subscriptionId);
        await syncStripeSubscription(subscription);
        return res.json({ received: true });
      }
      const userRef = await findUserRefByStripeCustomer(customerId);
      if (userRef) {
        await writeStripePremiumState(userRef.id, {
          isPremium: event.type === 'invoice.payment_succeeded',
          status: event.type === 'invoice.payment_failed' ? 'past_due' : 'active',
          stripeCustomerId: customerId,
          paymentProblem: event.type === 'invoice.payment_failed'
        });
      }
    }

    res.json({ received: true });
  } catch (err) {
    console.error('Stripe webhook handling failed:', err.message);
    res.status(500).send('webhook_failed');
  }
}

function sanitizeAIHistory(history) {
  if (!Array.isArray(history)) return [];
  return history
    .slice(-MAX_AI_HISTORY_ITEMS)
    .filter((item) => item && (item.role === 'user' || item.role === 'ai'))
    .map((item) => ({
      role: item.role === 'ai' ? 'assistant' : 'user',
      content: sanitizeMessageText(item && item.text, item.role === 'ai' ? 900 : 700)
    }))
    .filter((item) => item.content);
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetryableAIError(err) {
  const status = parseInt(err && (err.status || err.statusCode), 10);
  const msg = String((err && (err.message || err.code)) || '').toLowerCase();
  return !status || status === 408 || status === 409 || status === 425 || status === 429 || status >= 500
    || msg.includes('timeout')
    || msg.includes('aborted')
    || msg.includes('fetch')
    || msg.includes('overload')
    || msg.includes('temporarily');
}

function extractAIReply(response) {
  const choice = response && Array.isArray(response.choices) ? response.choices[0] : null;
  const message = choice && choice.message ? choice.message : {};
  if (typeof message.content === 'string') return sanitizeAIReplyText(message.content, 6000);
  if (Array.isArray(message.content)) {
    return sanitizeAIReplyText(message.content.map((part) => {
      if (!part) return '';
      if (typeof part === 'string') return part;
      return part.text || part.content || '';
    }).join('\n'), 6000);
  }
  return '';
}

function isUnhelpfulAIRefusal(reply) {
  const normalized = String(reply || '').toLowerCase();
  return /\b(i(?:'|’)?m sorry|i am sorry|cannot assist|can't assist|unable to assist|cannot help|can't help|not able to help)\b/.test(normalized);
}

async function createAICompletionWithFallback(messages, isAdvanced) {
  const models = isAdvanced ? AI_MODELS.advanced : AI_MODELS.standard;
  let lastErr = null;

  for (const model of models) {
    for (let attempt = 0; attempt < 1; attempt += 1) {
      try {
        const response = await openai.chat.completions.create({
          model,
          messages,
          temperature: isAdvanced ? 0.62 : 0.55,
          max_tokens: isAdvanced ? 900 : 620
        }, {
          timeout: AI_REQUEST_TIMEOUT_MS
        });
        const reply = extractAIReply(response);
        if (!reply) {
          const emptyErr = new Error('AI model returned an empty reply.');
          emptyErr.status = 502;
          throw emptyErr;
        }
        if (isUnhelpfulAIRefusal(reply)) {
          const refusalErr = new Error('AI model refused a safe user question.');
          refusalErr.status = 502;
          throw refusalErr;
        }
        return { response, model, reply };
      } catch (err) {
        lastErr = err;
        console.warn('AI model attempt failed:', model, 'attempt', attempt + 1, (err && err.message) || err);
        if (!isRetryableAIError(err) || attempt === 0) break;
        await wait(AI_RETRY_DELAY_MS * (attempt + 1));
      }
    }
  }

  throw lastErr || new Error('AI completion failed');
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
  const isAdvanced = hasPremiumAccess(userDoc);
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

function getCatchupWindowStart(nowUtc) {
  // User scans are paginated; every page needs enough lookback to cover a full rotation.
  return nowUtc.minus({ minutes: SCHEDULE_CATCHUP_MINUTES });
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
  res.json({
    ok: true,
    revision: String(process.env.RENDER_GIT_COMMIT || 'local').slice(0, 12),
    uptimeSeconds: Math.floor(process.uptime())
  });
});

app.get('/api/push/public-key', function(req, res) {
  res.json({ publicKey: process.env.VAPID_PUBLIC_KEY });
});

app.get('/api/access/status', async function(req, res) {
  const auth = await requireFirebaseAuth(req, res);
  if (!auth) return;

  try {
    const guard = await ensureTrialAndAccessState(auth, {
      timezone: req.query.timezone || ''
    });
    res.json({
      ok: true,
      access: serializeAccessState(guard.access),
      trialPopup: guard.popup,
      user: {
        uid: auth.uid,
        isAdmin: guard.access.isAdmin === true,
        hasAccess: guard.access.allowed === true,
        subscriptionStatus: guard.access.subscriptionStatus,
        trialEndDate: isoFromMillis(guard.access.trialEndMs)
      }
    });
  } catch (err) {
    console.error('Access status failed:', err.message);
    jsonError(res, 500, 'access_status_failed', 'Could not verify your free trial. Please try again.');
  }
});

app.post('/api/access/trial-popup-seen', async function(req, res) {
  const auth = await requireFirebaseAuth(req, res);
  if (!auth) return;

  const day = parseInt((req.body || {}).day, 10);
  if (!APP_TRIAL_POPUP_DAYS.has(day)) {
    return jsonError(res, 400, 'invalid_trial_popup_day', 'Invalid trial popup day.');
  }

  try {
    await db.collection('users').doc(auth.uid).set({
      trialPopupSeenDays: {
        [String(day)]: FieldValue.serverTimestamp()
      },
      updatedAt: FieldValue.serverTimestamp()
    }, { merge: true });
    res.json({ ok: true, day });
  } catch (err) {
    console.error('Trial popup seen failed:', err.message);
    jsonError(res, 500, 'trial_popup_seen_failed', 'Could not save this trial reminder.');
  }
});

app.post('/api/stripe/create-checkout-session', async function(req, res) {
  const auth = await requireFirebaseAuth(req, res);
  if (!auth) return;
  if (!stripeReady(res)) return;

  try {
    const requestBody = req.body || {};
    const wantsTrial = ALLOW_STRIPE_TRIAL_CHECKOUT && requestBody.trial === true && requestBody.skipTrial !== true && requestBody.checkoutMode !== 'direct';
    const selectedPlan = String(requestBody.plan || 'monthly').toLowerCase() === 'yearly' ? 'yearly' : 'monthly';
    const lineItem = getStripeLineItemForPlan(selectedPlan);
    const userRef = db.collection('users').doc(auth.uid);
    const userSnap = await userRef.get();
    const userDoc = userSnap.exists ? (userSnap.data() || {}) : {};
    const customerId = await ensureStripeCustomer(auth, userRef, userDoc);
    const access = buildAccessState(userDoc, DateTime.utc());
    const existingStatus = getSubscriptionStatus(userDoc);
    const hasStripeSubscription = !!(userDoc.stripeCustomerId && userDoc.stripeSubscriptionId);

    if ((access.state === 'subscribed' && userDoc.subscriptionPlan === 'stripe')
      || (hasStripeSubscription && ['active', 'trialing', 'past_due', 'unpaid'].includes(existingStatus))) {
      const portal = await stripe.billingPortal.sessions.create({
        customer: customerId,
        return_url: STRIPE_SUCCESS_URL
      });
      return res.json({ ok: true, url: portal.url, mode: 'portal' });
    }

    if (access.isAdmin === true || (access.state === 'subscribed' && userDoc.subscriptionPlan !== 'stripe')) {
      return res.json({ ok: true, alreadyPremium: true });
    }

    const subscriptionData = {
      metadata: {
        uid: auth.uid,
        app: 'jordyn-haircare',
        checkoutMode: wantsTrial ? 'trial' : 'direct',
        billingPlan: selectedPlan
      }
    };
    if (wantsTrial && STRIPE_TRIAL_DAYS > 0) subscriptionData.trial_period_days = STRIPE_TRIAL_DAYS;

    const session = await stripe.checkout.sessions.create({
      mode: 'subscription',
      customer: customerId,
      client_reference_id: auth.uid,
      line_items: [lineItem],
      allow_promotion_codes: true,
      success_url: addQueryParam(addQueryParam(STRIPE_SUCCESS_URL, 'stripe', 'success'), 'session_id', '{CHECKOUT_SESSION_ID}'),
      cancel_url: addQueryParam(STRIPE_CANCEL_URL, 'stripe', 'cancel'),
      subscription_data: subscriptionData,
      metadata: {
        uid: auth.uid,
        app: 'jordyn-haircare',
        product: 'premium',
        checkoutMode: wantsTrial ? 'trial' : 'direct',
        billingPlan: selectedPlan
      }
    });

    await userRef.set({
      stripeCheckoutSessionId: session.id,
      stripeCustomerId: customerId,
      updatedAt: FieldValue.serverTimestamp()
    }, { merge: true });

    res.json({ ok: true, url: session.url, sessionId: session.id, trialDays: wantsTrial ? STRIPE_TRIAL_DAYS : 0, checkoutMode: wantsTrial ? 'trial' : 'direct', plan: selectedPlan });
  } catch (err) {
    console.error('Stripe checkout failed:', err.message);
    jsonError(res, 500, 'stripe_checkout_failed', 'Could not open secure checkout. Try again.');
  }
});

app.post('/api/stripe/create-portal-session', async function(req, res) {
  const auth = await requireFirebaseAuth(req, res);
  if (!auth) return;
  if (!stripeReady(res)) return;

  try {
    const userSnap = await db.collection('users').doc(auth.uid).get();
    const userDoc = userSnap.exists ? (userSnap.data() || {}) : {};
    if (!userDoc.stripeCustomerId) {
      return jsonError(res, 404, 'stripe_customer_missing', 'No Stripe subscription was found for this account.');
    }

    const portal = await stripe.billingPortal.sessions.create({
      customer: userDoc.stripeCustomerId,
      return_url: STRIPE_SUCCESS_URL
    });

    res.json({ ok: true, url: portal.url });
  } catch (err) {
    console.error('Stripe portal failed:', err.message);
    jsonError(res, 500, 'stripe_portal_failed', 'Could not open billing portal. Try again.');
  }
});

app.post('/api/feedback', async function(req, res) {
  const auth = await requireFirebaseAuth(req, res);
  if (!auth) return;
  const accessGuard = await requireActiveAppAccess(req, res, auth);
  if (!accessGuard) return;

  try {
    const body = req.body || {};
    const message = sanitizeMessageText(body.message, 1400);
    const category = sanitizeMessageText(body.category || 'general', 80) || 'general';
    const source = sanitizeMessageText(body.source || 'app', 80) || 'app';
    if (!message) return jsonError(res, 400, 'missing_feedback', 'Please write your feedback first.');

    await db.collection('feedback').add({
      uid: auth.uid,
      email: auth.email || '',
      displayName: auth.name || '',
      category,
      source,
      message,
      userAgent: sanitizeMessageText(req.headers['user-agent'] || '', 300),
      createdAt: FieldValue.serverTimestamp(),
      status: 'new'
    });

    res.json({ ok: true });
  } catch (err) {
    console.error('Feedback save failed:', err.message);
    jsonError(res, 500, 'feedback_failed', 'Could not send feedback right now.');
  }
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
    pushSubscriptionCount: getPushSubscriptionCount(user),
    timezone: user.timezone || '',
    createdAt: safeTimestamp(user.createdAt),
    lastLoginAt: safeTimestamp(user.lastLoginAt),
    updatedAt: safeTimestamp(user.updatedAt)
  };
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function shortText(value, limit) {
  const text = String(value || '').replace(/\s+/g, ' ').trim();
  return text.length > limit ? text.slice(0, limit - 1) + '…' : text;
}

function safeTimestamp(value) {
  if (!value) return null;
  if (typeof value.toDate === 'function') return value.toDate().toISOString();
  if (value instanceof Date) return value.toISOString();
  if (typeof value === 'number') return new Date(value).toISOString();
  return value;
}

function compactReminder(reminder) {
  reminder = reminder || {};
  return {
    id: reminder.id || '',
    title: shortText(reminder.title || reminder.name || reminder.type || 'Reminder', 80),
    type: reminder.type || '',
    date: reminder.date || reminder.ds || reminder.day || '',
    time: reminder.time || '',
    frequency: reminder.frequency || reminder.freq || '',
    enabled: reminder.enabled !== false
  };
}

function compactProduct(product) {
  product = product || {};
  return {
    id: product.id || '',
    name: shortText(product.name || 'Product', 80),
    brand: shortText(product.brand || '', 60),
    category: product.category || product.cat || '',
    rating: product.rating || product.rate || null,
    inUse: product.inUse !== false && product.active !== false
  };
}

function latestByDate(items) {
  return safeArray(items).slice().sort(function(a, b) {
    const ad = new Date(a.date || a.ds || a.ts || 0).getTime() || 0;
    const bd = new Date(b.date || b.ds || b.ts || 0).getTime() || 0;
    return bd - ad;
  })[0] || null;
}

function buildAdminUserDetail(userSnap, dataSnap) {
  const user = userSnap.data() || {};
  const userData = dataSnap && dataSnap.exists ? (dataSnap.data() || {}) : {};
  const profile = userData.profile || {};
  const reminders = safeArray(userData.reminders);
  const products = safeArray(userData.products);
  const checkins = safeArray(userData.checkins);
  const growth = safeArray(userData.growth);
  const galleryCount = Math.max(
    safeArray(userData.gallery).length,
    Number((userData.galleryCloud || {}).itemCount || 0) || 0
  );
  const goals = safeArray(profile.hairGoals || profile.goals || profile.goal).filter(Boolean);

  return {
    uid: user.uid || userSnap.id,
    email: user.email || '',
    displayName: user.displayName || profile.name || '',
    isAdmin: user.isAdmin === true || ADMIN_EMAILS.has(String(user.email || '').toLowerCase()),
    isPremium: user.isPremium === true,
    subscriptionPlan: user.subscriptionPlan || '',
    notificationsEnabled: user.notificationsEnabled === true,
    timezone: user.timezone || profile.timezone || '',
    createdAt: safeTimestamp(user.createdAt),
    lastLoginAt: safeTimestamp(user.lastLoginAt),
    updatedAt: safeTimestamp(user.updatedAt || userData.updatedAt),
    profile: {
      name: profile.name || user.displayName || '',
      hairType: profile.hairType || '',
      hairColor: profile.hairColor || '',
      treatment: profile.treatment || '',
      hairGoal: profile.hairGoal || profile.goal || '',
      hairGoals: goals,
      lengthUnit: profile.lenUnit || profile.lengthUnit || '',
      temperatureUnit: profile.tempUnit || profile.temperatureUnit || '',
      monthlyGoal: profile.goalDays || profile.monthlyGoal || null
    },
    counts: {
      checkins: checkins.length,
      growth: growth.length,
      products: products.length,
      reminders: reminders.length,
      gallery: galleryCount,
      aiMessages: safeArray(userData.aiHistory).length
    },
    streak: userData.streak || profile.streak || 0,
    lastCheckin: latestByDate(checkins),
    lastGrowth: latestByDate(growth),
    activeProducts: products.filter(function(p) { return p && p.inUse !== false && p.active !== false; }).slice(0, 8).map(compactProduct),
    remindersPreview: reminders.slice(0, 12).map(compactReminder)
  };
}

app.get('/api/admin/users', async function(req, res) {
  const auth = await requireAdminAuth(req, res);
  if (!auth) return;

  const pageSize = Math.max(20, Math.min(80, parseInt(req.query.limit || '40', 10) || 40));
  const pageToken = String(req.query.pageToken || '').trim();
  const includeStats = String(req.query.includeStats || '') === '1';

  try {
    let query = db.collection('users').orderBy(FieldPath.documentId()).limit(pageSize);
    if (pageToken) query = query.startAfter(pageToken);
    const snap = await query.get();
    const users = snap.docs.map(publicUserSummary);
    const lastDoc = snap.docs[snap.docs.length - 1] || null;
    const nextPageToken = snap.size === pageSize && lastDoc ? lastDoc.id : '';

    const admins = ADMIN_EMAILS.size;
    const stats = {
      total: users.length + (nextPageToken ? '+' : ''),
      premium: null,
      admins,
      trial: null,
      notificationsOn: null,
      pushReady: null
    };

    if (includeStats) {
      const [total, premium, notificationsOn, pushReady] = await Promise.all([
        safeCount(db.collection('users')),
        safeCount(db.collection('users').where('isPremium', '==', true)),
        safeCount(db.collection('users').where('notificationsEnabled', '==', true)),
        safeCount(db.collection('users').where('pushSubscriptionCount', '>', 0))
      ]);
      stats.total = total;
      stats.premium = premium;
      stats.notificationsOn = notificationsOn;
      stats.pushReady = pushReady;
      stats.trial = total == null || premium == null ? null : Math.max(0, total - premium - admins);
    }

    res.json({
      ok: true,
      users,
      nextPageToken,
      stats
    });
  } catch (err) {
    console.error('Admin users failed:', err.message);
    jsonError(res, 500, 'admin_users_failed', 'Could not load users.');
  }
});

app.get('/api/admin/user-detail', async function(req, res) {
  const auth = await requireAdminAuth(req, res);
  if (!auth) return;

  const uid = String(req.query.uid || '').trim();
  if (!uid) return jsonError(res, 400, 'missing_uid', 'Missing user id.');

  try {
    const [userSnap, dataSnap] = await Promise.all([
      db.collection('users').doc(uid).get(),
      db.collection('userData').doc(uid).get()
    ]);
    if (!userSnap.exists) return jsonError(res, 404, 'user_not_found', 'User was not found.');
    res.json({ ok: true, detail: buildAdminUserDetail(userSnap, dataSnap) });
  } catch (err) {
    console.error('Admin user detail failed:', err.message);
    jsonError(res, 500, 'admin_user_detail_failed', 'Could not load user detail.');
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

  const requestBody = req.body || {};
  const body = sanitizeMessageText(requestBody.body || requestBody.message, 100);
  const clear = (req.body || {}).clear === true;

  try {
    if (clear) {
      await db.collection('broadcasts').doc('global').set({
        id: '',
        body: '',
        createdBy: '',
        createdByName: '',
        pushCompleteId: '',
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

    try {
      const pushStats = await processBroadcasts(true);
      res.json({ ok: true, broadcast: payload, pushStats });
    } catch (pushErr) {
      console.error('Admin broadcast push failed:', pushErr.message);
      await db.collection('broadcasts').doc('global').set({
        pushError: pushErr.message || 'Broadcast push failed',
        pushFailedAt: FieldValue.serverTimestamp()
      }, { merge: true });
      jsonError(res, 500, 'broadcast_push_failed', 'Broadcast was saved, but push delivery failed. Try again.');
    }
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
  const accessGuard = await requireActiveAppAccess(req, res, auth);
  if (!accessGuard) return;

  const { email, displayName, timezone, notificationsEnabled, subscription, clientDeviceId } = req.body || {};
  const uid = auth.uid;

  const cleaned = cleanSubscription(subscription, {
    clientDeviceId,
    userAgent: req.headers['user-agent'] || ''
  });
  if (!cleaned) return jsonError(res, 400, 'invalid_subscription', 'Missing or invalid push subscription.');
  const zone = getValidTimezone(timezone);
  const transfer = await transferPushDeviceOwnership(uid, cleaned, {
    email: auth.email || email || '',
    displayName: auth.name || displayName || '',
    timezone: zone
  });

  await mirrorNotificationPreferenceToUserData(uid, notificationsEnabled !== false, zone);

  res.json({ ok: true, count: transfer.count, detachedUsers: transfer.detachedUids.length });
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
  const subscriptions = prunePushSubscriptions((existing.pushSubscriptions || []).filter((sub) => sub && sub.endpoint !== endpoint));
  const deviceRef = db.collection('pushDevices').doc(getPushDeviceDocId(endpoint));
  const deviceSnap = await deviceRef.get();

  await userRef.set({
    notificationsEnabled: !!notificationsEnabled,
    pushSubscriptions: subscriptions,
    pushSubscriptionCount: subscriptions.length,
    updatedAt: FieldValue.serverTimestamp()
  }, { merge: true });

  if (!notificationsEnabled) {
    await mirrorNotificationPreferenceToUserData(uid, false, existing.timezone);
    await cancelAllRegisteredRemindersForUser(uid);
  }
  if (deviceSnap.exists && String((deviceSnap.data() || {}).uid || '') === uid) {
    await deviceRef.delete();
  }

  res.json({ ok: true });
});

app.post('/api/reminders/register', async function(req, res) {
  const auth = await requireFirebaseAuth(req, res);
  if (!auth) return;

  const body = req.body || {};
  const action = body.action === 'cancel' ? 'cancel' : 'upsert';
  const reminder = normalizeRegisteredReminder(body.reminder || {});
  if (!reminder.id) return jsonError(res, 400, 'missing_reminder_id', 'Missing reminder id.');

  if (action === 'cancel' || reminder.enabled === false) {
    await cancelDurableReminderSchedule(auth.uid, reminder.id);
    return res.json({ ok: true, action: 'cancelled' });
  }

  const accessGuard = await requireActiveAppAccess(req, res, auth);
  if (!accessGuard) return;

  const userDoc = accessGuard.userDoc || {};
  const zone = getValidTimezone(body.timezone || userDoc.timezone);
  const dueMoment = findNextReminderMoment(reminder, zone, DateTime.utc());
  if (dueMoment) {
    armRegisteredReminder(auth.uid, reminder, zone, DateTime.utc());
    await persistRegisteredReminderSchedule(auth.uid, reminder, zone, dueMoment);
  } else {
    await cancelDurableReminderSchedule(auth.uid, reminder.id);
  }
  res.json({
    ok: true,
    action: dueMoment ? 'scheduled' : 'fallback',
    exactTimer: !!(dueMoment && (dueMoment.toUTC().toMillis() - Date.now()) <= MAX_EXACT_REMINDER_DELAY_MS),
    dueAt: dueMoment ? dueMoment.toISO() : null
  });
});

app.post('/api/reminders/sync', async function(req, res) {
  const auth = await requireFirebaseAuth(req, res);
  if (!auth) return;
  const accessGuard = await requireActiveAppAccess(req, res, auth);
  if (!accessGuard) return;

  const body = req.body || {};
  const userDoc = accessGuard.userDoc || {};
  const zone = getValidTimezone(body.timezone || userDoc.timezone);
  const result = await reconcileRegisteredReminderSchedules(auth.uid, body.reminders, zone);
  res.json({ ok: true, scheduled: result.scheduled, removed: result.removed });
});

app.post('/api/chat', async function(req, res) {
  const auth = await requireFirebaseAuth(req, res);
  if (!auth) return;
  const accessGuard = await requireActiveAppAccess(req, res, auth);
  if (!accessGuard) return;

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
      : 'You have used all 20 free AI messages for this month.', {
      quota: formatQuotaResponse(quota)
    });
  }

  const responseContract = [
    sanitizeMessageText(context || 'You are a warm hair care expert.', 4000),
    quota.isAdvanced
      ? 'Advanced mode: be highly personal, specific, and premium-feeling. Use the user profile, logs, products, goals, and recent signals when present.'
      : 'Free mode: be warm and useful, but keep it shorter. Do not mention advanced-only audits or tell the user they are premium.',
    'Primary rule: answer exactly what the user asked in a simple ChatGPT-like way. Be direct first, then add only the useful extra details.',
    'Start with Quick Answer: or Direct Pick: and answer in 1-2 sentences. Then use only 1-3 short sections that directly help with the current question.',
    'Never repeat the same sentence, recommendation, or explanation in the opening and a later section. Each idea should appear once.',
    quota.isAdvanced
      ? 'Keep most answers around 180-260 words unless the user explicitly asks for detail.'
      : 'Keep most answers around 100-160 words unless the user explicitly asks for detail.',
    'Do not force every answer into Diagnosis, Routine, Product Picks, or Next Move. Choose labels from the question itself, or use no labels for a simple answer.',
    'Safe hair/scalp/cosmetic questions are allowed and must be answered: dandruff, flakes, itchy scalp, dry scalp, shampoo, conditioner, oil, steam, protein treatment, shedding, breakage, frizz, growth, product choice, brand choice, routine doubts, and emotional frustration about hair.',
    'Never refuse safe hair/scalp/cosmetic guidance. If there are medical red flags, give safe general guidance and recommend a dermatologist, but still answer the harmless part.',
	    'If the user asks which company, brand, shampoo, oil, product, mask, or treatment to use, give named picks or exact selection criteria first before any routine advice.',
	    'For direct product questions, name 3-5 real options when possible, then say which one to pick first. Do not hide behind generic advice.',
    'For purchase questions like "what shampoo should I buy" or "which company dandruff shampoo", do not use Diagnosis, Routine, Product Picks, or Next Move. Use Direct Pick:, Good Options:, How To Use:, Avoid: only if needed, and name actual options.',
    'Never ignore the object in the question. If they ask dandruff shampoo, answer dandruff shampoo. If they ask steam, answer steam. If they ask stress, answer stress.',
    'Do not title every answer Personalized Hair Plan or Wash Day Routine. The response title and section labels must match the exact user ask.',
    'Do not use old logs, goals, or products as the main answer when the user asks a specific topic like protein, steam, stress, or a simple non-hair question.',
    'Use adaptive mobile-card sections only when useful. For simple questions use Quick Answer:, Direct Pick:, Do This:, Avoid:. For treatment questions use When To Do It:, When To Skip It:, How To Use It:. Use Diagnosis:, Routine:, Product Picks:, Next Move: only when the user asks for analysis, routine, plan, or product comparison.',
    'If the prompt is vague or emotional, respond like a human first, then ask one sharp follow-up with options. Do not pretend certainty from old logs.',
    'If the prompt is not about hair, answer normally and do not inject hair profile context.',
    'Never answer a brand/product question with a generic scalp plan. Never answer a casual/emotional question with a full hair plan unless requested.',
    'Under each label write 1-3 short bullets or one tight paragraph. No markdown bold, no hashtags, no long essay paragraphs, no fake certainty, no "stay tuned", and no upgrade pitch unless the user explicitly asks about premium.',
    'Tone: supportive, personal, accurate, easy to read, and lightly emoji-led. Every line should feel useful.'
  ].join(' ');

  const messages = [{ role: 'system', content: responseContract }];
	  sanitizeAIHistory(history).forEach((m) => {
	    messages.push(m);
	  });
	  messages.push({
	    role: 'system',
	    content: 'The next user message is the current task. Answer that exact question directly. Do not repeat a previous topic, previous product, or generic routine unless the user asks for it.'
	  });
	  messages.push({ role: 'user', content: safeMessage });

  try {
    const completion = await createAICompletionWithFallback(messages, quota.isAdvanced);
    res.json({
      reply: completion.reply,
      quota: formatQuotaResponse(quota),
      tier: quota.isAdvanced ? 'advanced' : 'standard',
      model: completion.model,
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
  const windowStartUtc = getCatchupWindowStart(nowUtc);

  for (const userDocSnap of usersSnap.docs) {
    const userDoc = userDocSnap.data() || {};
    const uid = userDoc.uid || userDocSnap.id;
    let subscriptions = prunePushSubscriptions(userDoc.pushSubscriptions || []);
    if (!subscriptions.length) continue;

    const zone = getValidTimezone(userDoc.timezone);
    const localNow = nowUtc.setZone(zone);
    const localWindowStart = windowStartUtc.setZone(zone);

    const userDataSnap = await db.collection('userData').doc(uid).get();
    const userData = userDataSnap.exists ? (userDataSnap.data() || {}) : {};
	    const notificationMeta = pruneNotificationMeta(userDoc.notificationMeta || {}, nowUtc.toMillis());
	    const premiumAccess = hasPremiumAccess(userDoc);
	    if (!premiumAccess) continue;
	    const reminders = (userData.reminders || []).filter((item) => item && item.enabled !== false && (premiumAccess || item.smart !== true));
    let changed = false;

    for (const slot of SMART_NOTIFICATION_SLOTS) {
      if (!premiumAccess && slot.key !== 'evening') continue;
      const dueMoment = findDueLocalMomentForClock(slot.hour, slot.minute, localWindowStart, localNow);
      if (!dueMoment) continue;
      const logKey = `${dueMoment.toFormat('yyyy-LL-dd')}:${slot.key}`;
      if (notificationMeta[logKey]) continue;
      const hasSpecificReminder = reminders.some((reminder) => {
        if (!isReminderDueToday(reminder, dueMoment)) return false;
        const hm = String(reminder.time || '08:00').slice(0, 5).split(':');
        return (parseInt(hm[0], 10) || 0) === slot.hour && (parseInt(hm[1], 10) || 0) === slot.minute;
      });
      if (hasSpecificReminder) continue;

      const payload = buildSmartNotification(slot.key, userDoc, userData, dueMoment);
      if (!premiumAccess && payload.tag !== 'jhb-streak-save') continue;
      subscriptions = await sendToSubscriptions(subscriptions, payload);
      notificationMeta[logKey] = Date.now();
      changed = true;
    }

    if (changed) {
      await userDocSnap.ref.set({
        pushSubscriptions: subscriptions,
        pushSubscriptionCount: subscriptions.length,
        notificationMeta,
        updatedAt: FieldValue.serverTimestamp()
      }, { merge: true });
    }

    if (!reminders.length) continue;
    let durableIds = new Set();
    let durableLookupReady = true;
    try {
      const durableSnap = await db.collection('reminderSchedules').where('uid', '==', uid).limit(200).get();
      durableIds = new Set(durableSnap.docs.map((docSnap) => String((docSnap.data() || {}).reminderId || '')));
    } catch (err) {
      durableLookupReady = false;
      console.warn('Could not inspect durable reminders for user:', err.message);
    }
    if (!durableLookupReady) continue;

    for (const reminder of reminders) {
      if (!reminder.id || durableIds.has(String(reminder.id))) continue;
      const dueMoment = findDueReminderMoment(reminder, localWindowStart, localNow);
      if (dueMoment) {
        await persistRegisteredReminderSchedule(uid, reminder, zone, dueMoment);
        await deliverRegisteredReminder(uid, reminder.id, dueMoment.toISO(), reminder, zone);
        continue;
      }

      const nextDue = findNextReminderMoment(reminder, zone, nowUtc);
      if (!nextDue) continue;
      armRegisteredReminder(uid, reminder, zone, nowUtc);
      await persistRegisteredReminderSchedule(uid, reminder, zone, nextDue);
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
    if (!canUseScheduleFallback(err)) throw err;
    console.warn('Paginated schedule query needs a fallback:', err.message);
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
  if (broadcastRunning) return { running: true };
  broadcastRunning = true;
  const stats = {
    usersChecked: 0,
    subscribedUsers: 0,
    notificationsSent: 0,
    failedSubscriptions: 0,
    removedSubscriptions: 0,
    skippedAlreadySent: 0,
    skippedDuplicateEndpoints: 0
  };

  try {
    const broadcastSnap = await db.collection('broadcasts').doc('global').get();
    if (!broadcastSnap.exists) return stats;

    const broadcast = broadcastSnap.data() || {};
    if (!broadcast.id || !broadcast.body) return stats;
    if (!force && broadcast.id === lastBroadcastProcessedId) return stats;
    if (!force && broadcast.pushCompleteId === broadcast.id) {
      lastBroadcastProcessedId = broadcast.id;
      return stats;
    }
    if (!force && !broadcast.pushCompleteId && broadcast.pushedAt) {
      lastBroadcastProcessedId = broadcast.id;
      await broadcastSnap.ref.set({
        pushCompleteId: broadcast.id,
        dedupeMigratedAt: FieldValue.serverTimestamp()
      }, { merge: true });
      return stats;
    }

    // Broadcasts are low-frequency admin actions. Pulling subscribed user docs once
    // avoids fragile Firestore cursor/index combinations and is still safe for 3k+ users.
    const usersSnap = await db.collection('users').where('notificationsEnabled', '==', true).get();
    const docs = usersSnap.docs;

    for (let i = 0; i < docs.length; i += BROADCAST_BATCH_SIZE) {
      const batch = docs.slice(i, i + BROADCAST_BATCH_SIZE);
      await Promise.all(batch.map(async (userDocSnap) => {
        const userDoc = userDocSnap.data() || {};
        stats.usersChecked += 1;
        if (!hasPremiumAccess(userDoc)) return;
        const subscriptions = prunePushSubscriptions(userDoc.pushSubscriptions || []);
        if (!subscriptions.length) return;
        stats.subscribedUsers += 1;
        if (userDoc.lastBroadcastPushId === broadcast.id) {
          stats.skippedAlreadySent += 1;
          return;
        }

        const payload = {
          title: '📣 Hair Journal update',
          body: String(broadcast.body).slice(0, 100),
          tag: `jhb-broadcast-${broadcast.id}`,
          data: { page: 'Home', url: '/#open-page=Home', broadcastId: broadcast.id }
        };

        const result = await sendBroadcastToSubscriptionsOnce(broadcast.id, subscriptions, payload);
        stats.notificationsSent += result.stats.sent;
        stats.failedSubscriptions += result.stats.failed;
        stats.removedSubscriptions += result.stats.removed;
        stats.skippedDuplicateEndpoints += result.stats.skippedDuplicate;
        await userDocSnap.ref.set({
          pushSubscriptions: result.alive,
          pushSubscriptionCount: result.alive.length,
          lastBroadcastPushId: broadcast.id,
          updatedAt: FieldValue.serverTimestamp()
        }, { merge: true });
      }));
    }

    lastBroadcastProcessedId = broadcast.id;
    await db.collection('broadcasts').doc('global').set({
      pushStats: stats,
      pushCompleteId: broadcast.id,
      pushedAt: FieldValue.serverTimestamp(),
      pushError: FieldValue.delete(),
      pushFailedAt: FieldValue.delete()
    }, { merge: true });
    return stats;
  } finally {
    broadcastRunning = false;
  }
}

app.listen(PORT, () => {
  console.log('Server running on ' + PORT);
  startBackgroundLoop(
    'reminderSchedule',
    60000,
    processDurableReminderSchedules,
    15000
  );
  startBackgroundLoop(
    'schedule',
    SCHEDULE_SWEEP_INTERVAL_MS,
    processScheduledNotifications,
    BACKGROUND_JOB_INITIAL_DELAY_MS
  );
  startBackgroundLoop(
    'broadcast',
    BROADCAST_POLL_INTERVAL_MS,
    () => processBroadcasts(false),
    BACKGROUND_JOB_INITIAL_DELAY_MS + 15000
  );
});
