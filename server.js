require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { OpenAI } = require('openai');
const https = require('https');
const http = require('http');

const app = express();
const PORT = process.env.PORT || 3000;

const allowedOrigins = [
  'https://jordyn-haircare.web.app',
  'https://jordyn-haircare.firebaseapp.com',
  'http://localhost:5000',
  'http://127.0.0.1:5500'
];

app.use(cors({
  origin: function (origin, callback) {
    if (!origin || allowedOrigins.includes(origin)) return callback(null, true);
    return callback(new Error('Not allowed by CORS'));
  },
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));

app.use(express.json({ limit: '15kb' }));

if (!process.env.OPENAI_API_KEY) {
  console.error('OPENAI_API_KEY is not set.');
  process.exit(1);
}

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

app.post('/api/chat', async function (req, res) {
  const { message, context, uid, history = [] } = req.body;

  if (!message || !uid) return res.status(400).json({ error: 'Missing data' });

  const messages = [
    { role: 'system', content: context || 'You are a warm hair care expert.' }
  ];
  
  history.forEach(m => {
    messages.push({ role: m.role === 'ai' ? 'assistant' : 'user', content: m.text });
  });
  
  messages.push({ role: 'user', content: message });

  try {
    const response = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: messages,
      max_tokens: 300,
      temperature: 0.7
    });

    res.json({ reply: response.choices[0].message.content });
  } catch (err) {
    console.error('AI Error:', err.message);
    res.status(500).json({ error: 'AI unavailable right now.' });
  }
});

// Keep-alive for Render
const SELF_URL = process.env.RENDER_EXTERNAL_URL || ('http://localhost:' + PORT);
setInterval(() => {
  const client = SELF_URL.startsWith('https') ? https : http;
  client.get(SELF_URL, () => {}).on('error', () => {});
}, 14 * 60 * 1000);

app.listen(PORT, () => console.log('Server running on ' + PORT));
