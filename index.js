/**
 * Cloudflare Worker Telegram Bot & Fast Parquet Streaming API
 * Developer: @Maybechx
 * Zero Disk Storage - Pure Cloud-to-Cloud Streaming
 */

import { parquetMetadataAsync, parquetRead } from 'hyparquet';
import { decompress } from 'fzstd';

// Pure JavaScript ZSTD decompressor (Zero WASM, 100% Cloudflare Worker compliant)
const compressors = {
  ZSTD: (bytes) => decompress(bytes)
};

const DATASET_BASE_URL = 'https://huggingface.co/datasets/ansh21112/hitek-data-bucket/resolve/main';

/**
 * Resolves direct CloudFront CDN URL to avoid redirect roundtrips
 */
async function getDirectCdnUrl(url) {
  try {
    const head = await fetch(url, { method: 'HEAD', redirect: 'follow' });
    return { directUrl: head.url, byteLength: parseInt(head.headers.get('content-length') || '0', 10) };
  } catch (err) {
    return { directUrl: url, byteLength: 0 };
  }
}

function cleanPhone(val) {
  if (!val) return '';
  const digits = String(val).replace(/\D/g, '');
  return digits.length >= 10 ? digits.slice(-10) : digits;
}

/**
 * Streams and queries a single remote Parquet shard for a specific column and number
 */
async function queryParquetShard(shardName, targetCol, number, recordType) {
  const fileUrl = `${DATASET_BASE_URL}/${shardName}`;
  const { directUrl, byteLength } = await getDirectCdnUrl(fileUrl);
  if (!byteLength) return [];

  const asyncBuffer = {
    byteLength,
    async slice(start, end) {
      const res = await fetch(directUrl, {
        headers: { Range: `bytes=${start}-${end - 1}` }
      });
      return await res.arrayBuffer();
    }
  };

  try {
    const metadata = await parquetMetadataAsync(asyncBuffer);
    if (!metadata || !metadata.row_groups) return [];

    const target10 = cleanPhone(number);

    // Filter row groups using column statistics min/max
    const candidateRowGroups = [];
    for (let i = 0; i < metadata.row_groups.length; i++) {
      const rg = metadata.row_groups[i];
      const col = rg.columns.find(c => c.meta_data.path_in_schema[0] === targetCol);
      if (col && col.meta_data && col.meta_data.statistics) {
        const minVal = cleanPhone(col.meta_data.statistics.min || col.meta_data.statistics.min_value);
        const maxVal = cleanPhone(col.meta_data.statistics.max || col.meta_data.statistics.max_value);
        if (minVal && maxVal && minVal.length === 10 && maxVal.length === 10) {
          if (target10 >= minVal && target10 <= maxVal) {
            candidateRowGroups.push(i);
          }
        } else {
          candidateRowGroups.push(i);
        }
      } else {
        candidateRowGroups.push(i);
      }
    }

    const matchedRecords = [];
    const schemaCols = metadata.schema.slice(1).map(s => s.name);
    const targetColIdx = schemaCols.indexOf(targetCol);

    for (const rgIdx of candidateRowGroups) {
      await parquetRead({
        file: asyncBuffer,
        metadata,
        rowGroup: rgIdx,
        compressors,
        onComplete(data) {
          if (!data || !data[targetColIdx]) return;
          const colData = data[targetColIdx];
          for (let r = 0; r < colData.length; r++) {
            const rawVal = cleanPhone(colData[r]);
            if (rawVal === target10) {
              const record = {};
              schemaCols.forEach((colName, cIdx) => {
                record[colName] = data[cIdx] ? data[cIdx][r] : null;
              });
              record._record_type = recordType;
              matchedRecords.push(record);
            }
          }
        }
      });
    }

    return matchedRecords;
  } catch (err) {
    console.error(`[Shard Error] ${shardName}:`, err);
    return [];
  }
}

/**
 * Searches both Main and Alt shards in parallel
 */
async function searchPhoneNumber(number) {
  const cleanNumber = String(number).trim().replace(/\D/g, '');
  if (!cleanNumber || cleanNumber.length < 10 || cleanNumber.length > 15) {
    return {
      status: 'rejected',
      message: 'Invalid parameter. STRICTLY use 10 to 15 digit mobile number.',
      Developer: '@Maybechx'
    };
  }

  const lastDigit = cleanNumber.slice(-1);
  const primaryShard = `final_master_shard_${lastDigit}.parquet`;
  const altShard = `alt_master_shard_${lastDigit}.parquet`;

  // Parallel remote stream
  const [mainResults, altResults] = await Promise.all([
    queryParquetShard(primaryShard, 'mobile', cleanNumber, 'Main'),
    queryParquetShard(altShard, 'alt', cleanNumber, 'Alt')
  ]);

  const mainRecords = mainResults.map(r => { delete r._record_type; return r; });
  const altRecords = altResults.map(r => { delete r._record_type; return r; });

  if (mainRecords.length === 0 && altRecords.length === 0) {
    return {
      status: 'not_found',
      phone: cleanNumber,
      Developer: '@Maybechx'
    };
  }

  return {
    status: 'success',
    Data: {
      Main_Records: mainRecords,
      Alt_Records: altRecords
    },
    Developer: '@Maybechx'
  };
}

/**
 * Sends Telegram Bot API message
 */
async function sendTelegramMessage(botToken, chatId, text, parseMode = 'HTML') {
  if (!botToken) return;
  const url = `https://api.telegram.org/bot${botToken}/sendMessage`;
  try {
    await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: chatId,
        text: text,
        parse_mode: parseMode
      })
    });
  } catch (e) {
    console.error('[Telegram Send Error]', e);
  }
}

/**
 * Sends Telegram Bot Chat Action (typing...)
 */
async function sendTelegramAction(botToken, chatId, action = 'typing') {
  if (!botToken) return;
  try {
    await fetch(`https://api.telegram.org/bot${botToken}/sendChatAction`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, action: action })
    });
  } catch (e) {}
}

/**
 * Formats data records into clean Telegram HTML
 */
function formatTelegramResponse(phone, result, devTag = '@poojaxyz1') {
  if (result.status === 'not_found') {
    return `❌ <b>No records found for:</b> <code>${phone}</code>\n\n👨‍💻 <b>Developer:</b> ${devTag}`;
  }
  if (result.status === 'rejected') {
    return `⚠️ <b>Error:</b> ${result.message}\n\n👨‍💻 <b>Developer:</b> ${devTag}`;
  }

  let msg = `🔍 <b>Search Results for:</b> <code>${phone}</code>\n`;
  msg += `━━━━━━━━━━━━━━━━━━━━━\n\n`;

  const main = result.Data?.Main_Records || [];
  const alt = result.Data?.Alt_Records || [];

  if (main.length > 0) {
    msg += `📱 <b>[ MAIN RECORDS (${main.length}) ]</b>\n`;
    main.forEach((rec, idx) => {
      msg += `<b>#${idx + 1}</b>\n`;
      msg += `👤 <b>Name:</b> ${rec.name ? rec.name.trim() : 'N/A'}\n`;
      msg += `👨 <b>Father Name:</b> ${rec.fname ? rec.fname.trim() : 'N/A'}\n`;
      msg += `📞 <b>Mobile:</b> <code>${rec.mobile ? rec.mobile.trim() : 'N/A'}</code>\n`;
      msg += `📲 <b>Alt:</b> ${rec.alt ? rec.alt.trim() : 'N/A'}\n`;
      msg += `📍 <b>Address:</b> ${rec.address ? rec.address.trim() : 'N/A'}\n`;
      msg += `🌐 <b>Circle:</b> ${rec.circle ? rec.circle.trim() : 'N/A'}\n`;
      msg += `🆔 <b>ID:</b> ${rec.id ? rec.id.trim() : 'N/A'}\n`;
      msg += `📧 <b>Email:</b> ${rec.email ? rec.email.trim() : 'N/A'}\n\n`;
    });
  }

  if (alt.length > 0) {
    msg += `📑 <b>[ ALTERNATE RECORDS (${alt.length}) ]</b>\n`;
    alt.forEach((rec, idx) => {
      msg += `<b>#${idx + 1}</b>\n`;
      msg += `👤 <b>Name:</b> ${rec.name ? rec.name.trim() : 'N/A'}\n`;
      msg += `👨 <b>Father Name:</b> ${rec.fname ? rec.fname.trim() : 'N/A'}\n`;
      msg += `📞 <b>Mobile:</b> <code>${rec.mobile ? rec.mobile.trim() : 'N/A'}</code>\n`;
      msg += `📲 <b>Alt:</b> ${rec.alt ? rec.alt.trim() : 'N/A'}\n`;
      msg += `📍 <b>Address:</b> ${rec.address ? rec.address.trim() : 'N/A'}\n`;
      msg += `🌐 <b>Circle:</b> ${rec.circle ? rec.circle.trim() : 'N/A'}\n`;
      msg += `🆔 <b>ID:</b> ${rec.id ? rec.id.trim() : 'N/A'}\n\n`;
    });
  }

  msg += `👨‍💻 <b>Developer:</b> ${devTag}`;
  return msg;
}

/**
 * Main Cloudflare Worker Fetch Handler
 */
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const method = request.method;
    const botToken = env.TELEGRAM_BOT_TOKEN;
    const devTag = env.DEVELOPER || '@poojaxyz1';

    // 1. CORS Preflight
    if (method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type'
        }
      });
    }

    // 2. Telegram Webhook Handler (POST / or POST /webhook)
    if (method === 'POST') {
      try {
        const update = await request.json();
        const msg = update.message || update.edited_message;

        if (msg && msg.chat && msg.text) {
          const chatId = msg.chat.id;
          const text = msg.text.trim();

          if (text === '/start' || text === '/help') {
            const welcomeText = `👋 <b>Welcome to Hitek Data Bot!</b>\n\n` +
              `Send any <b>10 to 15 digit mobile number</b> to search live records in real-time.\n\n` +
              `<i>Example:</i> <code>1400500510</code>\n\n` +
              `👨‍💻 <b>Developer:</b> ${devTag}`;
            await sendTelegramMessage(botToken, chatId, welcomeText);
            return new Response('OK', { status: 200 });
          }

          // Extract number from message
          const numberMatch = text.match(/\d{10,15}/);
          if (!numberMatch) {
            await sendTelegramMessage(botToken, chatId, '⚠️ <i>Please send a valid 10 to 15 digit phone number.</i>');
            return new Response('OK', { status: 200 });
          }

          const targetPhone = numberMatch[0];
          
          // Send typing indicator in background
          if (ctx && ctx.waitUntil) {
            ctx.waitUntil(sendTelegramAction(botToken, chatId, 'typing'));
          }

          // Perform live on-demand cloud search
          const searchResult = await searchPhoneNumber(targetPhone);
          searchResult.Developer = devTag;
          const replyText = formatTelegramResponse(targetPhone, searchResult, devTag);

          await sendTelegramMessage(botToken, chatId, replyText);
          return new Response('OK', { status: 200 });
        }

        return new Response('OK', { status: 200 });
      } catch (err) {
        console.error('[Webhook Error]', err);
        return new Response('Error', { status: 500 });
      }
    }

    // 3. HTTP REST API: GET /FetchData?Number=XXXXXXXXXX
    if (url.pathname === '/FetchData') {
      const number = url.searchParams.get('Number');
      if (!number) {
        return new Response(JSON.stringify({
          status: 'rejected',
          message: 'Invalid parameter. STRICTLY use /FetchData?Number=XXXXXXXXXX',
          Developer: devTag
        }), {
          status: 400,
          headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
        });
      }

      const result = await searchPhoneNumber(number);
      result.Developer = devTag;
      const statusCode = result.status === 'success' ? 200 : (result.status === 'not_found' ? 404 : 400);

      return new Response(JSON.stringify(result), {
        status: statusCode,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
      });
    }

    // 4. Landing Page / Status
    if (url.pathname === '/' || url.pathname === '/health') {
      return new Response(JSON.stringify({
        status: 'online',
        service: 'Hitek Telegram Bot & Cloudflare Data Worker',
        endpoints: {
          telegram_webhook: 'POST /',
          rest_api: 'GET /FetchData?Number=XXXXXXXXXX'
        },
        Developer: devTag
      }), {
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
      });
    }

    return new Response(JSON.stringify({
      status: 'rejected',
      message: 'Invalid endpoint. STRICTLY use /FetchData?Number=XXXXXXXXXX',
      Developer: devTag
    }), {
      status: 404,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }
    });
  }
};
