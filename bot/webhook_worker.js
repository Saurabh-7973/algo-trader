/**
 * webhook_worker.js — Cloudflare Worker (v3)
 *
 * TWO modes:
 *   1. fetch()     — handles Telegram webhook messages (/scan, /help, etc.)
 *   2. scheduled() — Cloudflare Cron Trigger fires exactly at 9:15 AM IST
 *                    and triggers GitHub Actions via repository_dispatch
 *
 * Why this fixes the timing:
 *   GitHub scheduled cron jobs are delayed 2-3 hours during peak times.
 *   Cloudflare Cron Triggers fire within seconds of the scheduled time.
 *   repository_dispatch (used here) runs GitHub Actions immediately — no queue.
 *
 * Setup:
 *   In Cloudflare Dashboard → Workers → your worker → Settings → Triggers
 *   → Add Cron Trigger: "45 3 * * 1-5"  (3:45 AM UTC = 9:15 AM IST, Mon-Fri)
 */

const CACHE_TTL_MS = 5 * 60 * 1000;
let usersCache = null;
let cacheTime  = 0;

export default {

  // ── Telegram webhook handler ───────────────────────────────────────────────
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

    const cmd = text.split(" ")[0].toLowerCase().split("@")[0];

    // Admin commands
    if (chatId === String(env.ADMIN_CHAT_ID)) {
      if (cmd === "/approve") {
        const targetId = text.split(" ")[1];
        await send(env, chatId, targetId
          ? `To approve ${targetId}:\nAdd to Users tab → ChatID: ${targetId} | Status: ACTIVE`
          : "Usage: /approve <chatId>"
        );
        return new Response("OK", { status: 200 });
      }
      if (cmd === "/users") {
        const users = await getUsers(env, true);
        const lines = users.length
          ? users.map((u,i) => `${i+1}. ${u.chatId} — ${u.name} [${u.status}]`).join("\n")
          : "No active users.";
        await send(env, chatId, `Active Users (${users.length})\n\n${lines}`);
        return new Response("OK", { status: 200 });
      }
    }

    // Auth check
    const users = await getUsers(env);
    const user  = users.find(u => u.chatId === chatId);

    if (!user || user.status?.toUpperCase() === "BLOCKED") {
      if (!user) {
        await send(env, String(env.ADMIN_CHAT_ID),
          `New access request\nName: ${username}\nChatID: ${chatId}\nAdd to Users tab with Status: ACTIVE`
        );
      }
      await send(env, chatId,
        `Access not yet granted.\nRequest sent to admin.\nYour Chat ID: ${chatId}`
      );
      return new Response("OK", { status: 200 });
    }

    // Commands
    if (cmd === "/start" || cmd === "/help") {
      await send(env, chatId,
        `Welcome, ${firstName}!\n\n` +
        `/scan — Run scan now\n` +
        `/status — System status\n` +
        `/help — This message\n\n` +
        `Auto-scan runs every trading day at 9:15 AM IST.`
      );
    }
    else if (cmd === "/scan") {
      await send(env, chatId, `Scan triggered. Results in ~3 minutes.`);
      await triggerGitHub(env, chatId, "scan");
    }
    else if (cmd === "/status") {
      await send(env, chatId,
        `System Status\n\nBot: Online\nSchedule: 9:15 AM IST Mon-Fri\nMode: Paper`
      );
    }
    else {
      await send(env, chatId, `Unknown command. Type /help`);
    }

    return new Response("OK", { status: 200 });
  },

  // ── Cloudflare Cron Trigger handler ───────────────────────────────────────
  // Fires at exactly 3:45 AM UTC = 9:15 AM IST (set in Cloudflare dashboard)
  // This bypasses GitHub's unreliable scheduled cron entirely.
  async scheduled(event, env, ctx) {
    console.log(`Cron fired: ${new Date().toISOString()}`);
    try {
      await triggerGitHub(env, "", "daily_scan");
      console.log("Daily scan triggered via Cloudflare Cron");
    } catch (e) {
      console.error(`Cron trigger failed: ${e}`);
    }
  },
};

// ── Helpers ───────────────────────────────────────────────────────────────────

async function getUsers(env, forceRefresh = false) {
  const now = Date.now();
  if (!forceRefresh && usersCache && (now - cacheTime) < CACHE_TTL_MS) {
    return usersCache;
  }
  try {
    const resp = await fetch(env.USERS_CSV_URL);
    const csv  = await resp.text();
    const rows = csv.trim().split("\n").slice(1);
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
  if (!chatId) return;
  await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, text }),
  });
}

async function triggerGitHub(env, requestingChatId, eventType) {
  const [owner, repo] = (env.GITHUB_REPO || "").split("/");
  const response = await fetch(
    `https://api.github.com/repos/${owner}/${repo}/dispatches`,
    {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${env.GITHUB_TOKEN}`,
        "Accept": "application/vnd.github+json",
        "Content-Type": "application/json",
        "X-GitHub-Api-Version": "2022-11-28",
      },
      body: JSON.stringify({
        event_type: eventType,
        client_payload: { requesting_chat_id: requestingChatId },
      }),
    }
  );

  if (!response.ok) {
    const err = await response.text();
    console.error(`GitHub dispatch failed: ${response.status} — ${err}`);
    if (requestingChatId) {
      await send(env, requestingChatId, `Scan failed to start. GitHub error: ${response.status}`);
    }
  }
}
