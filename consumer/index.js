if (process.env.PUBSUB_EMULATOR_HOST) {
  process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0'; // Only disable TLS verification for local emulator testing
}

const { PubSub } = require('@google-cloud/pubsub');
const express = require('express');

const { OAuth2Client } = require('google-auth-library');
const authClient = new OAuth2Client();

const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 8080;

app.use(express.json());

const pubsubOptions = {};
if (process.env.PUBSUB_EMULATOR_HOST) {
  pubsubOptions.projectId = 'local-project';
}
const pubsub = new PubSub(pubsubOptions);
const subscriptionName = 'verify-sub';
let currentSubscription = null;

const isLocal = !!process.env.PUBSUB_EMULATOR_HOST;
let throttlingDelay = 3; // スロットリング遅延（秒）の初期値
let messagePromiseChain = Promise.resolve(); // メッセージの直列化処理用Promiseチェーン
let trackingMessages = []; // 現在Pub/Subキューに滞留中または処理中のメッセージ: [{ id, text, status: 'PUBLISHED'|'PROCESSING', timestamp }]
let isConsumerRunning = true; // コンシューマーの稼働状態フラグ


// In-memory logs history (keep last 30 messages)
const messageHistory = [];
let processedCount = 0;

// Read dashboard HTML once on startup
const dashboardHtml = fs.readFileSync(path.join(__dirname, 'dashboard.html'), 'utf8');

// GCPメタデータサーバーからプロジェクトIDとOAuth2アクセストークンを取得するヘルパー
async function getGCPToken() {
  const metadataHeaders = { headers: { 'Metadata-Flavor': 'Google' } };
  
  // プロジェクトIDの取得
  const projectIdRes = await fetch('http://metadata.google.internal/computeMetadata/v1/project/project-id', metadataHeaders);
  if (!projectIdRes.ok) {
    throw new Error(`Failed to get project ID from metadata server: ${projectIdRes.statusText}`);
  }
  const projectId = (await projectIdRes.text()).trim();
  
  // アクセストークンの取得
  const tokenRes = await fetch('http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token', metadataHeaders);
  if (!tokenRes.ok) {
    throw new Error(`Failed to get access token from metadata server: ${tokenRes.statusText}`);
  }
  const tokenData = await tokenRes.json();
  
  return { projectId, accessToken: tokenData.access_token };
}

// API to check pipeline status
app.get('/api/status', (req, res) => {
  res.json({
    status: isConsumerRunning ? 'ACTIVE' : 'PAUSED',
    mode: isLocal ? 'LOCAL (Simulation)' : 'PRODUCTION',
    processedCount,
    subscription: subscriptionName,
    throttlingDelay
  });
});

// API to pause consumer receiving messages
app.post('/api/consumer/pause', async (req, res) => {
  try {
    if (isLocal) {
      await stopConsumer();
    }
    isConsumerRunning = false;
    res.json({ success: true, status: 'PAUSED' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// API to resume consumer receiving messages
app.post('/api/consumer/resume', async (req, res) => {
  try {
    if (isLocal) {
      startConsumer();
    }
    isConsumerRunning = true;
    res.json({ success: true, status: 'ACTIVE' });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 設定更新API
app.post('/api/settings', (req, res) => {
  const { delay } = req.body;
  if (typeof delay === 'number' && delay >= 0) {
    throttlingDelay = delay;
    res.json({ success: true, throttlingDelay });
  } else {
    res.status(400).json({ error: 'Invalid delay value' });
  }
});

// API to publish a new message into the Pub/Sub topic
app.post('/api/publish', async (req, res) => {
  try {
    const { id, text } = req.body;
    if (!text) {
      return res.status(400).json({ error: 'Text is required' });
    }
    const payload = {
      id: id || Date.now(),
      text: text,
      timestamp: new Date().toISOString()
    };
    
    // トラッキングリストに追加（初期状態は PUBLISHED = Pub/Sub内滞留中）
    trackingMessages.push({
      id: payload.id,
      text: payload.text,
      status: 'PUBLISHED',
      timestamp: payload.timestamp
    });

    const dataBuffer = Buffer.from(JSON.stringify(payload));
    const messageId = await pubsub.topic('verify-topic').publishMessage({ data: dataBuffer });
    res.json({ success: true, messageId, id: payload.id });
  } catch (err) {
    console.error('[Consumer API] Failed to publish message:', err);
    res.status(500).json({ error: err.message });
  }
});

// API to get processing logs history
app.get('/api/history', (req, res) => {
  res.json(messageHistory);
});

// API to get currently active un-acked messages in consumer memory
app.get('/api/queue', (req, res) => {
  res.json(trackingMessages);
});

// Cloud RunのPushサブスクリプション用エンドポイント
app.post('/translate', async (req, res) => {
  if (!process.env.PUBSUB_EMULATOR_HOST) {
    const bearer = req.header('Authorization');
    if (!bearer || !bearer.startsWith('Bearer ')) {
      console.warn('[Consumer] Missing or invalid Authorization header. Strictly enforcing OIDC verification in production.');
      return res.status(401).send('Missing or invalid Authorization header');
    }
    const token = bearer.split(' ')[1];
    try {
      const ticket = await authClient.verifyIdToken({
        idToken: token,
        audience: process.env.OIDC_AUDIENCE || `https://${req.get('host')}/translate`,
      });
      const payload = ticket.getPayload();
      console.log(`[Consumer] OIDC Token verified successfully. Email: ${payload.email}`);
    } catch (e) {
      console.error('[Consumer] OIDC verification failed:', e);
      return res.status(403).send('Invalid token');
    }
  }

  const pubSubMessage = req.body.message;
  if (!pubSubMessage || !pubSubMessage.data) {
    return res.status(400).send('Invalid message payload');
  }

  // Pub/Subメッセージオブジェクトのインターフェースに合わせたモックを作成してハンドラーに渡す
  const mockMessage = {
    id: pubSubMessage.messageId || pubSubMessage.id,
    data: Buffer.from(pubSubMessage.data, 'base64'),
    attributes: pubSubMessage.attributes || {},
    publishTime: pubSubMessage.publishTime,
    ack: () => {
      if (!res.headersSent) {
        res.status(200).send('OK');
      }
    },
    nack: () => {
      if (!res.headersSent) {
        res.status(500).send('Error');
      }
    }
  };

  messageHandler(mockMessage);
});

// Serve Dashboard HTML Page
app.get('/', (req, res) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  res.send(dashboardHtml);
});

console.log(`[Consumer] Started. Mode: ${isLocal ? 'LOCAL (Simulation)' : 'PRODUCTION'}`);
console.log('[Consumer] Listening for events from Pub/Sub...');

// メッセージハンドラ関数定義
const messageHandler = (message) => {
  messagePromiseChain = messagePromiseChain.then(async () => {
    const startTime = Date.now();
    let payload;
    try {
      const rawData = message.data.toString();
      try {
        payload = JSON.parse(rawData);
      } catch (e) {
        payload = {
          id: 'legacy-' + message.id,
          text: rawData,
          timestamp: new Date(startTime).toISOString()
        };
      }

      // 一時停止状態であれば、即座に nack して物理キューに戻す（処理中ステータスにしない）
      if (!isConsumerRunning) {
        console.log(`[Consumer] Paused. Returning message ID: ${payload.id} to queue via nack().`);
        
        // メモリ上のステータスを PUBLISHED（滞留中）に戻す、または存在しなければ追加する
        const msg = trackingMessages.find(m => String(m.id) === String(payload.id));
        if (msg) {
          msg.status = 'PUBLISHED';
        } else {
          trackingMessages.push({
            id: payload.id,
            text: payload.text,
            status: 'PUBLISHED',
            timestamp: payload.timestamp || new Date(startTime).toISOString()
          });
        }
        
        message.nack();
        return;
      }
      
      // トラッキングリストのステータスを PROCESSING（処理中）に更新
      const msg = trackingMessages.find(m => String(m.id) === String(payload.id));
      if (msg) {
        msg.status = 'PROCESSING';
      } else {
        // 直接CLIからパブリッシュされた場合のフォールバック追加
        trackingMessages.push({
          id: payload.id,
          text: payload.text,
          status: 'PROCESSING',
          timestamp: new Date(startTime).toISOString()
        });
      }

      console.log(`\n--- [Consumer] Received Event ID: ${payload.id} ---`);
      console.log(`[Consumer] Input Text: "${payload.text}"`);
      
      // スロットリング遅延の実行
      if (throttlingDelay > 0) {
        console.log(`[Consumer] Throttling: sleeping for ${throttlingDelay} seconds...`);
        await new Promise(resolve => setTimeout(resolve, throttlingDelay * 1000));
      }
      
      const translateStartTime = Date.now();
      let summary = '';
      
      if (isLocal) {
        // ローカル環境動作時は、高速化のため無料の Google 翻訳 API を呼び出す
        const hasJapanese = /[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\uff66-\uff9f]/.test(payload.text);
        const sourceLang = 'auto';
        const targetLang = hasJapanese ? 'en' : 'ja';
        console.log(`[Consumer] Translating text locally (${sourceLang} -> ${targetLang}) using Google Translate API...`);
        const url = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=${sourceLang}&tl=${targetLang}&dt=t&q=${encodeURIComponent(payload.text)}`;
        const response = await fetch(url);
        if (!response.ok) {
          throw new Error(`Google Translate API returned status ${response.status}`);
        }
        const json = await response.json();
        summary = json[0].map(item => item[0]).join('');
      } else {
        // 本番環境（GCP）動作時は本物の Vertex AI (Gemini 3.5 Flash) API を呼び出す
        console.log(`[Consumer] Connecting to Vertex AI in us-central1 (Translation Mode)...`);
        const { projectId, accessToken } = await getGCPToken();
        const region = process.env.GCP_REGION || 'us-central1';
        const model = process.env.GEMINI_MODEL || 'gemini-3.5-flash';
        const apiUrl = `https://${region}-aiplatform.googleapis.com/v1/projects/${projectId}/locations/${region}/publishers/google/models/${model}:generateContent`;
        
        const prompt = "Detect the language of the input text. If it is English, translate it to natural Japanese. If it is Japanese, translate it to natural English. Output ONLY the translated text, no introductory or concluding remarks.";
        const requestBody = {
          contents: [{
            role: 'user',
            parts: [{ text: `${prompt}\n\n${payload.text}` }]
          }]
        };
        
        const response = await fetch(apiUrl, {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${accessToken}`,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(requestBody)
        });
        
        if (!response.ok) {
          const errText = await response.text();
          throw new Error(`Vertex AI API returned status ${response.status}: ${errText}`);
        }
        
        const resData = await response.json();
        summary = resData.candidates?.[0]?.content?.parts?.[0]?.text || 'No translation returned';
      }
      
      const endTime = Date.now();
      const duration = endTime - translateStartTime;
      console.log(`[Consumer] Translation: "${summary.trim()}"`);
      console.log(`[Consumer] Pure Translation Duration: ${duration}ms`);
      console.log(`[Consumer] Saving result to local store / GCS simulated path...`);
      console.log(`-----------------------------------------------\n`);
      
      const publishedTime = payload.timestamp ? new Date(payload.timestamp).getTime() : startTime;
      const waitDuration = (translateStartTime - publishedTime) / 1000;
      const processDuration = (endTime - translateStartTime) / 1000;
      const totalDuration = (endTime - publishedTime) / 1000;
      
      // メッセージ履歴に保存（重複排除）
      const existingIndex = messageHistory.findIndex(m => m.id === payload.id);
      const logData = {
        id: payload.id,
        text: payload.text,
        publishedAt: payload.timestamp || new Date(publishedTime).toISOString(),
        startedAt: new Date(translateStartTime).toISOString(),
        endedAt: new Date(endTime).toISOString(),
        waitDuration: parseFloat(waitDuration.toFixed(2)),
        processDuration: parseFloat(processDuration.toFixed(2)),
        totalDuration: parseFloat(totalDuration.toFixed(2)),
        summary: summary.trim()
      };
      if (existingIndex !== -1) {
        messageHistory[existingIndex] = logData;
      } else {
        messageHistory.unshift(logData);
        if (messageHistory.length > 30) messageHistory.pop();
        processedCount++;
      }
      
      // 処理が完了したので ack し、トラッキングリストから削除
      trackingMessages = trackingMessages.filter(m => String(m.id) !== String(payload.id));
      message.ack(); // メッセージの処理成功をキューに通知
    } catch (error) {
      console.error('[Consumer] Error processing message:', error);
      if (payload && payload.id) {
        // エラー時はステータスを再び PUBLISHED（滞留中）に戻す
        const msg = trackingMessages.find(m => String(m.id) === String(payload.id));
        if (msg) {
          msg.status = 'PUBLISHED';
        }
      }
      message.nack(); // 失敗したため再配送を要求
    }
  }).catch(err => {
    console.error('[Consumer] Promise chain exception:', err);
  });
};

function startConsumer() {
  if (!currentSubscription) {
    currentSubscription = pubsub.subscription(subscriptionName, {
      flowControl: {
        maxMessages: 1
      }
    });
    currentSubscription.on('message', messageHandler);
    currentSubscription.on('error', (error) => {
      console.error('[Consumer] Subscription error:', error);
      if (error.message && error.message.includes('does not exist')) {
        console.log('[Consumer] Subscription verify-sub does not exist yet. Retrying in 5 seconds...');
        stopConsumer();
        setTimeout(startConsumer, 5000);
      }
    });
    console.log('[Consumer] Started listening for events from Pub/Sub.');
  }
}

async function stopConsumer() {
  if (currentSubscription) {
    currentSubscription.removeAllListeners('message');
    currentSubscription.removeAllListeners('error');
    try {
      await currentSubscription.close();
    } catch (e) {
      console.error('[Consumer] Error closing subscription:', e);
    }
    currentSubscription = null;
    console.log('[Consumer] Stopped listening for events from Pub/Sub (Paused).');
  }
}

// 初期起動時の開始（ローカルエミュレータ起動時のみ自動的にPullリスナーを開始する）
if (isLocal) {
  startConsumer();
} else {
  console.log('[Consumer] Running in PRODUCTION mode (Push subscription endpoint active).');
}

// Expressサーバーの起動
app.listen(PORT, '0.0.0.0', () => {
  console.log(`[Consumer] Dashboard Web Server running on port ${PORT}`);
});