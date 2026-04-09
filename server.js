require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { GoogleGenerativeAI } = require('@google/generative-ai');
const https = require('https');
const http = require('http');

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
  origin: function (origin, callback) {
    if (!origin) return callback(null, true);
    if (allowedOrigins.includes(origin)) return callback(null, true);
    return callback(new Error('Not allowed by CORS: ' + origin));
  },
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use(express.json({ limit: '10kb' }));

if (!process.env.GEMINI_API_KEY) {
  console.error('GEMINI_API_KEY is not set.');
  process.exit(1);
}

const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

const model = genAI.getGenerativeModel({
  model: 'gemini-2.5-flash',
  generationConfig: {
    maxOutputTokens: 300,
    temperature: 0.75
  }
});

app.get('/', (req, res) => {
  res.json({
    status: 'ok',
    service: 'Hair Journal Tracker AI Backend'
  });
});

app.get('/test-gemini', async (req, res) => {
  try {
    const result = await model.generateContent('Say hello in one short sentence.');
    const reply = result && result.response ? result.response.text() : '';

    res.json({
      ok: true,
      reply: reply
    });
  } catch (err) {
    console.error('TEST GEMINI FULL ERROR:', err);
    console.error('TEST GEMINI MESSAGE:', err && err.message);
    console.error('TEST GEMINI STACK:', err && err.stack);

    res.status(500).json({
      ok: false,
      message: (err && err.message) || 'Unknown Gemini error'
    });
  }
});

app.post('/api/chat', async (req, res) => {
  const { message, context, uid } = req.body;

  if (!message || typeof message !== 'string' || message.trim().length === 0) {
    return res.status(400).json({ error: 'Message is required.' });
  }

  if (message.length > 500) {
    return res.status(400).json({ error: 'Message too long. Max 500 characters.' });
  }

  if (!uid) {
    return res.status(400).json({ error: 'User ID is required.' });
  }

  const systemContext =
    context && typeof context === 'string'
      ? context
      : 'You are a warm, expert hair care assistant. Keep responses under 3 sentences.';

  const fullPrompt = systemContext + '\n\nUser: ' + message.trim();

  try {
    const result = await model.generateContent(fullPrompt);
    const text = result && result.response ? result.response.text() : '';

    if (!text) {
      return res.status(500).json({ error: 'AI returned an empty response.' });
    }

    return res.json({ reply: text });
  } catch (err) {
    console.error('GEMINI FULL ERROR:', err);
    console.error('GEMINI MESSAGE:', err && err.message);
    console.error('GEMINI STACK:', err && err.stack);

    if (err && err.message && (err.message.includes('RESOURCE_EXHAUSTED') || err.message.toLowerCase().includes('quota'))) {
      return res.status(429).json({ error: 'AI quota reached. Please try again tomorrow.' });
    }

    if (err && err.message && err.message.includes('API_KEY_INVALID')) {
      return res.status(401).json({ error: 'Invalid API key.' });
    }

    return res.status(500).json({
      error: (err && err.message) || 'AI is temporarily unavailable. Try again in a moment.'
    });
  }
});

const SELF_URL = process.env.RENDER_EXTERNAL_URL || ('http://localhost:' + PORT);

setInterval(function () {
  try {
    const client = SELF_URL.startsWith('https') ? https : http;
    client.get(SELF_URL, function () {}).on('error', function () {});
  } catch (e) {}
}, 14 * 60 * 1000);

app.listen(PORT, function () {
  console.log('Server running on port ' + PORT);
  console.log('API key configured: ' + (process.env.GEMINI_API_KEY ? 'YES' : 'NO'));
});
