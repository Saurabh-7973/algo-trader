/**
 * webhook_worker.js — Cloudflare Worker (v2)
 *
 * User management via Google Sheets "Users" tab.
 * No hardcoded IDs. No secret editing. No redeploys.
 *
 * To add a user: Add their ChatID to the Users tab in Google Sheet → done.
 * To remove:     Delete their row → done.
 *
 * Google Sheet "Users" tab columns:
 *   ChatID | Name | Status | Added On
 *   (Status: ACTIVE or BLOCKED)
 *
 * The tab must be published as CSV:
 *   Sheet → File → Share → Publish to web → Users tab → CSV format → Publish
 *   Copy the CSV URL → set as USERS_CSV_URL env var in Cloudflare Worker
 *
 * Cloudflare Worker env vars (set in dashboard):
 *   TELEGRAM_BOT_TOKEN  — bot token
 *   GITHUB_TOKEN        — GitHub PAT (repo scope)
 *   GITHUB_REPO         — "Saurabh-7973/algo-trader"
 *   ADMIN_CHAT_ID       — Saurabh's chat ID (gets registration notifications)
 *   USERS_CSV_URL       — published CSV URL of the Users tab
 */

const CACHE_TTL_MS = 5 * 60 * 1000; // cache users list for 5 min
let usersCache = null;
let cacheTime  = 0;

export default {
  async fetch(request, env) {
    if (request.method !== "POST") {
      return new Response("Algo Trader Bot — Online", { status: 200 });
    }

    let body;
    try { body = await request.json(); }
    catch { return new Response("OK", { status: 200 }); }

    const message = body?.message;
    if (!message) return new Response("OK", { status: 200 });

    const chatId    = String(message.chat?.id || "");
    const text      = (message.text || "").trim();
    const firstName = message.from?.first_name || "";
    const username  = message.from?.username
      ? `@${message.from.username}` : firstName || "Unknown";

    const cmd = text.split(" ")[0].toLowerCase().replace("@", "").split("@")[0];

    // ── Admin commands (no auth check needed) ─────────────────────
    if (chatId === String(env.ADMIN_CHAT_ID)) {
      if (cmd === "/approve") {
        const targetId = text.split(" ")[1];
        if (!targetId) {
          await send(env, chatId, "Usage: /approve &lt;chatId&gt;");
        } else {
          await send(env, chatId,
            `✅ To approve user <code>${targetId}</code>:\n\n` +
            `1. Open your Google Sheet → Users tab\n` +
            `2. Add a new row:\n` +
            `   ChatID: <code>${targetId}</code>\n` +
            `   Status: <code>ACTIVE</code>\n\n` +
            `Access will work within 5 minutes.`
          );
        }
        return new Response("OK", { status: 200 });
      }

      if (cmd === "/users") {
        const users = await getUsers(env, true); // force refresh
        if (!users.length) {
          await send(env, chatId, "No users in the Users tab yet.");
        } else {
          const lines = users.map((u, i) =>
            `${i+1}. <code>${u.chatId}</code> — ${u.name} [${u.status}]`
          ).join("\n");
          await send(env, chatId, `<b>Active Users (${users.length})</b>\n\n${lines}`);
        }
        return new Response("OK", { status: 200 });
      }
    }

    // ── Auth check for all other commands ─────────────────────────
    const users  = await getUsers(env);
    const user   = users.find(u => u.chatId === chatId);
    const status = user?.status?.toUpperCase();

    if (!user || status === "BLOCKED") {
      if (!user) {
        // Unknown user — notify admin
        await send(env, String(env.ADMIN_CHAT_ID),
          `🔔 <b>New access request</b>\n\n` +
          `Name: ${username}\n` +
          `ChatID: <code>${chatId}</code>\n\n` +
          `To approve, add to Users tab:\n` +
          `ChatID: <code>${chatId}</code> · Status: <code>ACTIVE</code>\n\n` +
          `Or reply: /approve ${chatId}`
        );
      }
      await send(env, chatId,
        `⛔ <b>Access not yet granted.</b>\n\n` +
        `Your request has been sent to the admin.\n` +
        `You'll be able to use the bot once approved.\n\n` +
        `Your Chat ID: <code>${chatId}</code>`
      );
      return new Response("OK", { status: 200 });
    }

    // ── Commands ──────────────────────────────────────────────────
    if (cmd === "/start" || cmd === "/help") {
      await send(env, chatId,
        `👋 Welcome, ${firstName}!\n\n` +
        `<b>Available Commands</b>\n\n` +
        `/scan — Run full Nifty scan now\n` +
        `/status — System status\n` +
        `/help — This message\n\n` +
        `<i>Auto-scan runs every trading day at 9:15 AM IST.</i>`
      );
    }

    else if (cmd === "/scan") {
      await send(env, chatId,
        `🔍 <b>Scan triggered</b>\n\n` +
        `Running full scan...\n` +
        `Results in ~3 minutes.`
      );
      await triggerScan(env, chatId);
    }

    else if (cmd === "/status") {
      await send(env, chatId,
        `✅ <b>System Status</b>\n\n` +
        `Bot: Online\nSchedule: 9:15 AM IST Mon–Fri\n` +
        `Mode: Paper (signal only)\nBasket: Nifty 500`
      );
    }

    else {
      await send(env, chatId, `Unknown command. Type /help for options.`);
    }

    return new Response("OK", { status: 200 });
  }
};

// ── Helpers ───────────────────────────────────────────────────────

async function getUsers(env, forceRefresh = false) {
  const now = Date.now();
  if (!forceRefresh && usersCache && (now - cacheTime) < CACHE_TTL_MS) {
    return usersCache;
  }
  try {
    const resp = await fetch(env.USERS_CSV_URL);
    const csv  = await resp.text();
    const rows = csv.trim().split("\n").slice(1); // skip header
    usersCache = rows
      .map(row => {
        const cols = row.split(",").map(c => c.trim().replace(/^"|"$/g, ""));
        return { chatId: cols[0], name: cols[1] || "", status: cols[2] || "ACTIVE" };
      })
      .filter(u => u.chatId && u.status.toUpperCase() !== "BLOCKED");
    cacheTime = now;
    return usersCache;
  } catch (e) {
    console.error("Failed to fetch users:", e);
    return usersCache || [];
  }
}

async function send(env, chatId, text) {
  await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, text, parse_mode: "HTML" }),
  });
}

async function triggerScan(env, requestingChatId) {
  const [owner, repo] = (env.GITHUB_REPO || "").split("/");
  await fetch(`https://api.github.com/repos/${owner}/${repo}/dispatches`, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${env.GITHUB_TOKEN}`,
      "Accept": "application/vnd.github+json",
      "Content-Type": "application/json",
      "X-GitHub-Api-Version": "2022-11-28",
    },
    body: JSON.stringify({
      event_type: "scan",
      client_payload: { requesting_chat_id: requestingChatId },
    }),
  });
}
