require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { GoogleGenerativeAI } = require('@google/generative-ai');

const app = express();
const PORT = process.env.PORT || 3000;

// ── CORS ─────────────────────────────────────────────────────────
// Allow requests from your Firebase Hosting domain only.
// During development, also allow localhost.
const allowedOrigins = [
  'https://jordyn-haircare.web.app',
  'https://jordyn-haircare.firebaseapp.com',
  'https://hair-care-tracker.web.app',
  'http://localhost:5000',
  'http://127.0.0.1:5500'
];

app.use(cors({
  origin: function (origin, callback) {
    // Allow requests with no origin (mobile apps, curl, Postman)
    if (!origin) return callback(null, true);
    if (allowedOrigins.includes(origin)) return callback(null, true);
    callback(new Error('Not allowed by CORS'));
  },
  methods: ['POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use(express.json({ limit: '10kb' }));

// ── GEMINI SETUP ─────────────────────────────────────────────────
if (!process.env.GEMINI_API_KEY) {
  console.error('❌  GEMINI_API_KEY is not set. Edit your .env file or Render environment variables.');
  process.exit(1);
}

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
const model = genAI.getGenerativeModel({
  model: 'gemini-1.5-flash-latest',
  generationConfig: {
    maxOutputTokens: 300,
    temperature: 0.75
  }
});
  

// ── HEALTH CHECK ─────────────────────────────────────────────────
app.get('/', (req, res) => {
  res.json({ status: 'ok', service: 'Hair Journal Tracker AI Backend' });
});

// ── AI CHAT ENDPOINT ─────────────────────────────────────────────
// POST /api/chat
// Body: { message: string, context: string, uid: string }
app.post('/api/chat', async (req, res) => {
  const { message, context, uid } = req.body;

  // Basic validation
  if (!message || typeof message !== 'string' || message.trim().length === 0) {
    return res.status(400).json({ error: 'Message is required.' });
  }
  if (message.length > 500) {
    return res.status(400).json({ error: 'Message too long. Max 500 characters.' });
  }
  if (!uid) {
    return res.status(400).json({ error: 'User ID is required.' });
  }

  const systemContext = context && typeof context === 'string'
    ? context
    : 'You are a warm, expert hair care assistant. Keep responses under 3 sentences.';

  const fullPrompt = systemContext + '\n\nUser: ' + message.trim();

  try {
    const result = await model.generateContent(fullPrompt);
    const response = result.response;
    const text = response.text();

    if (!text) {
      return res.status(500).json({ error: 'AI returned an empty response.' });
    }

    return res.json({ reply: text });

  } catch (err) {
    console.error('Gemini full error:', err);
console.error('Gemini message:', err?.message);
console.error('Gemini stack:', err?.stack);

    if (err.message?.includes('RESOURCE_EXHAUSTED') || err.message?.includes('quota')) {
      return res.status(429).json({ error: 'AI quota reached. Please try again tomorrow.' });
    }
    if (err.message?.includes('API_KEY_INVALID')) {
      return res.status(401).json({ error: 'Invalid API key.' });
    }

    return res.status(500).json({ error: 'AI is temporarily unavailable. Try again in a moment.' });
  }
});

// Keep-alive ping — prevents Render free tier from sleeping
const SELF_URL = process.env.RENDER_EXTERNAL_URL || 'http://localhost:' + PORT;
setInterval(function() {
  require('https').get(SELF_URL).on('error', function(){});
}, 14 * 60 * 1000); // ping every 14 minutes


// ── START ─────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`\n✅  Server running → http://localhost:${PORT}`);
  console.log(`   API key configured: ${process.env.GEMINI_API_KEY ? 'YES ✅' : 'NO — edit .env file'}\n`);
});
