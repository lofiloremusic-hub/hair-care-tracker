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

// Primary model
const model = genAI.getGenerativeModel({
  model: 'gemini-2.5-flash-lite',
  generationConfig: {
    maxOutputTokens: 300,
    temperature: 0.75
  }
});

// Optional fallback model
const fallbackModel = genAI.getGenerativeModel({
  model: 'gemini-2.5-flash',
  generationConfig: {
    maxOutputTokens: 300,
    temperature: 0.75
  }
});

function sleep(ms) {
  return new Promise(function (resolve) {
    setTimeout(resolve, ms);
  });
}

async function generateWithRetry(prompt, retries, delayMs) {
  let lastError = null;

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      console.log('Gemini attempt:', attempt);

      const result = await model.generateContent(prompt);
      return result;
    } catch (err) {
      lastError = err;
      const msg = (err && err.message ? err.message : '').toLowerCase();

      const isRetryable =
        msg.includes('503') ||
        msg.includes('service unavailable') ||
        msg.includes('high demand') ||
        msg.includes('overloaded') ||
        msg.includes('unavailable');

      console.error('Primary model failed on attempt', attempt, '-', err && err.message);

      if (!isRetryable) {
        throw err;
      }

      if (attempt < retries) {
        await sleep(delayMs * attempt);
      }
    }
  }

  console.log('Trying fallback model...');
  const fallbackResult = await fallbackModel.generateContent(prompt);
  return fallbackResult;
}

app.get('/', function (req, res) {
  res.json({
    status: 'ok',
    service: 'Hair Journal Tracker AI Backend'
  });
});

app.get('/test-gemini', async function (req, res) {
  try {
    const result = await generateWithRetry('Say hello in one short sentence.', 3, 2000);
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

app.post('/api/chat', async function (req, res) {
  const message = req.body.message;
  const context = req.body.context;
  const uid = req.body.uid;

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
    const result = await generateWithRetry(fullPrompt, 3, 2000);
    const text = result && result.response ? result.response.text() : '';

    if (!text) {
      return res.status(500).json({ error: 'AI returned an empty response.' });
    }

    return res.json({ reply: text });
  } catch (err) {
    console.error('GEMINI FULL ERROR:', err);
    console.error('GEMINI MESSAGE:', err && err.message);
    console.error('GEMINI STACK:', err && err.stack);

    const errorMessage = err && err.message ? err.message.toLowerCase() : '';

    if (errorMessage.includes('resource_exhausted') || errorMessage.includes('quota')) {
      return res.status(429).json({
        error: 'AI quota reached. Please try again tomorrow.'
      });
    }

    if (errorMessage.includes('api_key_invalid')) {
      return res.status(401).json({
        error: 'Invalid API key.'
      });
    }

    if (
      errorMessage.includes('503') ||
      errorMessage.includes('service unavailable') ||
      errorMessage.includes('high demand') ||
      errorMessage.includes('overloaded')
    ) {
      return res.status(503).json({
        error: 'The AI is busy right now. Please try again in a few seconds.'
      });
    }

    return res.status(500).json({
      error: 'AI is temporarily unavailable. Try again in a moment.'
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
