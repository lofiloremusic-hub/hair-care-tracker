require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { OpenAI } = require('openai');
const https = require('https');
const http = require('http');
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
const AI_REQUEST_TIMEOUT_MS = Math.max(20000, Math.min(120000, parseInt(process.env.AI_REQUEST_TIMEOUT_MS || '70000', 10) || 70000));
const AI_RETRY_DELAY_MS = Math.max(300, Math.min(5000, parseInt(process.env.AI_RETRY_DELAY_MS || '900', 10) || 900));
const SCHEDULE_CATCHUP_MINUTES = Math.max(12, parseInt(process.env.SCHEDULE_CATCHUP_MINUTES || '24', 10) || 24);
const SCHEDULE_SWEEP_LIMIT = Math.max(50, Math.min(500, parseInt(process.env.SCHEDULE_SWEEP_LIMIT || '250', 10) || 250));
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
const STRIPE_SUCCESS_URL = (process.env.STRIPE_SUCCESS_URL || 'https://jordyn-haircare.web.app/#open-page=Premium').trim();
const STRIPE_CANCEL_URL = (process.env.STRIPE_CANCEL_URL || 'https://jordyn-haircare.web.app/#open-page=Premium').trim();
const stripe = STRIPE_SECRET_KEY ? new Stripe(STRIPE_SECRET_KEY) : null;

function parseAIModels(value, fallback) {
  const seen = new Set();
  return String(value || '')
    .split(',')
    .concat(fallback || [])
    .map((model) => String(model || '').trim())
    .filter((model) => {
      if (!model || seen.has(model)) return false;
      seen.add(model);
      return true;
    });
}

const AI_MODELS = {
  advanced: parseAIModels(process.env.AI_ADVANCED_MODELS || process.env.AI_MODEL_ADVANCED, ['openrouter/auto']),
  standard: parseAIModels(process.env.AI_STANDARD_MODELS || process.env.AI_MODEL_STANDARD || process.env.AI_MODEL_FREE, ['openrouter/auto'])
};

let lastScheduleSweepAt = 0;
let scheduleCursorId = '';
let scheduleRunning = false;
let broadcastRunning = false;
let lastBroadcastProcessedId = '';
const MAX_PUSH_SUBSCRIPTIONS_PER_USER = Math.max(1, Math.min(5, parseInt(process.env.MAX_PUSH_SUBSCRIPTIONS_PER_USER || '1', 10) || 1));

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
    body: 'Your scheduled hair reminder is ready. Tap to open Hair Journal and stay consistent.',
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

function hasPremiumAccess(userDoc) {
  return !!(userDoc && (userDoc.isAdmin === true || userDoc.isPremium === true));
}

function sanitizeMessageText(value, maxLength) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, maxLength);
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
  const isPremium = state.isPremium === true;

  await db.runTransaction(async (tx) => {
    const snap = await tx.get(userRef);
    const existing = snap.exists ? (snap.data() || {}) : {};

    if (!isPremium && existing.isAdmin === true) return;
    if (!isPremium && existing.subscriptionPlan && existing.subscriptionPlan !== 'stripe') return;
    if (!isPremium && state.stripeSubscriptionId && existing.stripeSubscriptionId && existing.stripeSubscriptionId !== state.stripeSubscriptionId) return;

    tx.set(userRef, compactDefined({
      isPremium,
      subscriptionPlan: isPremium ? 'stripe' : null,
      premiumSource: isPremium ? 'stripe' : null,
      premiumStatus: state.status || null,
      stripeCustomerId: state.stripeCustomerId || existing.stripeCustomerId || undefined,
      stripeSubscriptionId: state.stripeSubscriptionId || existing.stripeSubscriptionId || null,
      stripePriceId: state.stripePriceId || existing.stripePriceId || null,
      stripeCancelAtPeriodEnd: state.cancelAtPeriodEnd === undefined ? null : !!state.cancelAtPeriodEnd,
      stripeCurrentPeriodEnd: state.currentPeriodEnd || null,
      stripeTrialEnd: state.trialEnd || null,
      premiumPaymentProblem: state.paymentProblem === undefined ? existing.premiumPaymentProblem : !!state.paymentProblem,
      updatedAt: FieldValue.serverTimestamp()
    }), { merge: true });
  });

  await db.collection('userData').doc(uid).set({
    profile: compactDefined({
      premium: isPremium,
      isPremium,
      subscriptionPlan: isPremium ? 'stripe' : null,
      premiumStatus: state.status || null,
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
  const accessStatuses = new Set(['active', 'trialing', 'past_due']);
  const firstItem = subscription.items && subscription.items.data && subscription.items.data[0];
  const priceId = firstItem && firstItem.price && firstItem.price.id;

  await writeStripePremiumState(uid, {
    isPremium: accessStatuses.has(status),
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
      const userRef = await findUserRefByStripeCustomer(customerId);
      if (userRef) {
        await userRef.set({
          premiumPaymentProblem: event.type === 'invoice.payment_failed',
          premiumStatus: event.type === 'invoice.payment_failed' ? 'payment_failed' : 'active',
          updatedAt: FieldValue.serverTimestamp()
        }, { merge: true });
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
  // Do not feed old assistant cards back into the model; that causes repeated canned templates.
  return history
    .slice(-MAX_AI_HISTORY_ITEMS)
    .filter((item) => item && item.role !== 'ai')
    .slice(-6)
    .map((item) => ({
      role: 'user',
      content: sanitizeMessageText(item && item.text, 700)
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

async function createAICompletionWithFallback(messages, isAdvanced) {
  const models = isAdvanced ? AI_MODELS.advanced : AI_MODELS.standard;
  let lastErr = null;

  for (const model of models) {
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        const response = await openai.chat.completions.create({
          model,
          messages,
          temperature: isAdvanced ? 0.74 : 0.62,
          max_tokens: isAdvanced ? 1200 : 780
        }, {
          timeout: AI_REQUEST_TIMEOUT_MS
        });
        return { response, model };
      } catch (err) {
        lastErr = err;
        console.warn('AI model attempt failed:', model, 'attempt', attempt + 1, (err && err.message) || err);
        if (!isRetryableAIError(err) || attempt === 1) break;
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

app.post('/api/stripe/create-checkout-session', async function(req, res) {
  const auth = await requireFirebaseAuth(req, res);
  if (!auth) return;
  if (!stripeReady(res)) return;

  try {
    const requestBody = req.body || {};
    const wantsTrial = requestBody.trial !== false && requestBody.skipTrial !== true && requestBody.checkoutMode !== 'direct';
    const selectedPlan = String(requestBody.plan || 'monthly').toLowerCase() === 'yearly' ? 'yearly' : 'monthly';
    const lineItem = getStripeLineItemForPlan(selectedPlan);
    const userRef = db.collection('users').doc(auth.uid);
    const userSnap = await userRef.get();
    const userDoc = userSnap.exists ? (userSnap.data() || {}) : {};
    const customerId = await ensureStripeCustomer(auth, userRef, userDoc);

    if (userDoc.isPremium === true && userDoc.subscriptionPlan === 'stripe') {
      const portal = await stripe.billingPortal.sessions.create({
        customer: customerId,
        return_url: STRIPE_SUCCESS_URL
      });
      return res.json({ ok: true, url: portal.url, mode: 'portal' });
    }

    if (userDoc.isPremium === true || userDoc.isAdmin === true) {
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

  const { email, displayName, timezone, notificationsEnabled, subscription, clientDeviceId } = req.body || {};
  const uid = auth.uid;

  const cleaned = cleanSubscription(subscription, {
    clientDeviceId,
    userAgent: req.headers['user-agent'] || ''
  });
  if (!cleaned) return jsonError(res, 400, 'invalid_subscription', 'Missing or invalid push subscription.');
  const userRef = db.collection('users').doc(uid);
  const snap = await userRef.get();
  const existing = snap.exists ? (snap.data() || {}) : {};
  const existingSubscriptions = (existing.pushSubscriptions || []).filter((sub) => {
    if (!sub || !sub.endpoint) return false;
    return !(cleaned.clientDeviceId && sub.clientDeviceId === cleaned.clientDeviceId);
  });
  const subscriptions = prunePushSubscriptions([].concat(existingSubscriptions, cleaned).filter(Boolean));

  await userRef.set({
    uid,
    email: auth.email || email || existing.email || '',
    displayName: auth.name || displayName || existing.displayName || '',
    timezone: getValidTimezone(timezone || existing.timezone),
    notificationsEnabled: notificationsEnabled !== false,
    pushSubscriptions: subscriptions,
    pushSubscriptionCount: subscriptions.length,
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
  const subscriptions = prunePushSubscriptions((existing.pushSubscriptions || []).filter((sub) => sub && sub.endpoint !== endpoint));

  await userRef.set({
    notificationsEnabled: !!notificationsEnabled,
    pushSubscriptions: subscriptions,
    pushSubscriptionCount: subscriptions.length,
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
      : 'You have used all 20 free AI messages for this month.', {
      quota: formatQuotaResponse(quota)
    });
  }

  const responseContract = [
    sanitizeMessageText(context || 'You are a warm hair care expert.', 4000),
    quota.isAdvanced
      ? 'Advanced mode: be highly personal, specific, and premium-feeling. Use the user profile, logs, products, goals, and recent signals when present.'
      : 'Free mode: be warm and useful, but keep it shorter. Do not mention advanced-only audits or tell the user they are premium.',
    'Answer the exact user question first, like ChatGPT would: direct, accurate, and natural. Do not force every answer into the same routine/product template.',
    'Use the smallest useful structure. If the user asks one direct thing, give one direct answer with only the sections that help that exact thing.',
    'If the user asks which company, brand, shampoo, oil, product, mask, or treatment to use, give named picks or exact selection criteria first before any routine advice.',
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
  messages.push({ role: 'user', content: safeMessage });

  try {
    const completion = await createAICompletionWithFallback(messages, quota.isAdvanced);
    const response = completion.response;
    res.json({
      reply: response.choices[0].message.content,
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
  const windowStartUtc = getCatchupWindowStart(nowUtc, lastScheduleSweepAt);
  lastScheduleSweepAt = nowUtc.toMillis();

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
    let changed = false;

    if (premiumAccess) {
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
    }

    const reminders = (userData.reminders || []).filter((item) => item && item.enabled !== false && (premiumAccess || item.smart !== true));
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
        pushSubscriptionCount: subscriptions.length,
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
  if (broadcastRunning) return { running: true };
  broadcastRunning = true;
  const stats = {
    usersChecked: 0,
    subscribedUsers: 0,
    notificationsSent: 0,
    failedSubscriptions: 0,
    removedSubscriptions: 0,
    skippedAlreadySent: 0
  };

  try {
    const broadcastSnap = await db.collection('broadcasts').doc('global').get();
    if (!broadcastSnap.exists) return stats;

    const broadcast = broadcastSnap.data() || {};
    if (!broadcast.id || !broadcast.body) return stats;
    if (!force && broadcast.id === lastBroadcastProcessedId) return stats;

    // Broadcasts are low-frequency admin actions. Pulling subscribed user docs once
    // avoids fragile Firestore cursor/index combinations and is still safe for 3k+ users.
    const usersSnap = await db.collection('users').where('notificationsEnabled', '==', true).get();
    const docs = usersSnap.docs;

    for (let i = 0; i < docs.length; i += BROADCAST_BATCH_SIZE) {
      const batch = docs.slice(i, i + BROADCAST_BATCH_SIZE);
      await Promise.all(batch.map(async (userDocSnap) => {
        const userDoc = userDocSnap.data() || {};
        const subscriptions = prunePushSubscriptions(userDoc.pushSubscriptions || []);
        stats.usersChecked += 1;
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

        const result = await sendToSubscriptionsWithStats(subscriptions, payload);
        stats.notificationsSent += result.stats.sent;
        stats.failedSubscriptions += result.stats.failed;
        stats.removedSubscriptions += result.stats.removed;
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
      pushedAt: FieldValue.serverTimestamp(),
      pushError: FieldValue.delete(),
      pushFailedAt: FieldValue.delete()
    }, { merge: true });
    return stats;
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
