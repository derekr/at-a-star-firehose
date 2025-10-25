import { Hono } from "hono";
import { streamSSE } from "hono/streaming";
import { on } from "node:events";
import { EventEmitter } from "node:events";
import { Database } from "bun:sqlite";
import { raw } from "hono/html";

const MIN_FILTER_LENGTH = 3;
const FLUSH_POSTS_MS = 300;
const POST_RETENTION_MS = 60 * 60 * 1000;
const CLEANUP_POSTS_INTERVAL_MS = 60000;

interface Post {
  author: string;
  text: string;
  timestamp: string;
  uri: string;
}

interface Session {
  id: string;
  name: string;
}

interface Tab {
  id: string;
  sessionId: string;
  filter: string;
}

const db = new Database("firehose.db");
db.run(`
  CREATE TABLE IF NOT EXISTS posts (
    uri TEXT PRIMARY KEY,
    author TEXT NOT NULL,
    text TEXT NOT NULL,
    timestamp INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_timestamp ON posts(timestamp);
  CREATE INDEX IF NOT EXISTS idx_text ON posts(text);
`);

const sessions = new Map<string, Session>();
const tabs = new Map<string, Tab>();
const events = new EventEmitter();

let batchBuffer: Post[] = [];

function generateSessionName(): string {
  const adjectives = [
    "happy",
    "curious",
    "dancing",
    "sleepy",
    "brave",
    "clever",
    "gentle",
    "wild",
    "quiet",
    "loud",
  ];
  const animals = [
    "penguin",
    "octopus",
    "dolphin",
    "panda",
    "fox",
    "owl",
    "koala",
    "tiger",
    "rabbit",
    "bear",
  ];
  const adj = adjectives[Math.floor(Math.random() * adjectives.length)];
  const animal = animals[Math.floor(Math.random() * animals.length)];
  return `${adj}-${animal}`;
}

function getOrCreateSession(sessionId?: string): Session {
  if (sessionId && sessions.has(sessionId)) {
    return sessions.get(sessionId)!;
  }
  const session = { id: crypto.randomUUID(), name: generateSessionName() };
  sessions.set(session.id, session);
  return session;
}

function getOrCreateTab(tabId: string | undefined, sessionId: string): Tab {
  if (tabId && tabs.has(tabId)) {
    return tabs.get(tabId)!;
  }
  const tab = { id: tabId || crypto.randomUUID(), sessionId, filter: "" };
  tabs.set(tab.id, tab);
  return tab;
}

function addPost(post: Post) {
  batchBuffer.push(post);
  if (batchBuffer.length >= 100) {
    flushPosts();
  }
}

function flushPosts() {
  if (batchBuffer.length === 0) return;

  const insertStmt = db.prepare(
    "INSERT OR IGNORE INTO posts (uri, author, text, timestamp) VALUES (?, ?, ?, ?)",
  );
  const insertMany = db.transaction((posts: Post[]) => {
    for (const post of posts) {
      const timestamp = new Date(post.timestamp).getTime();
      insertStmt.run(post.uri, post.author, post.text, timestamp);
    }
  });

  insertMany(batchBuffer);
  batchBuffer = [];
  events.emit("posts.added");
}

function cleanupOldPosts() {
  const cutoff = Date.now() - POST_RETENTION_MS;
  db.prepare("DELETE FROM posts WHERE timestamp < ?").run(cutoff);
}

function queryPosts(filter: string): Post[] {
  const stmt = filter
    ? db.prepare(
        "SELECT * FROM posts WHERE text LIKE ? ORDER BY timestamp DESC LIMIT 100",
      )
    : db.prepare("SELECT * FROM posts ORDER BY timestamp DESC LIMIT 100");

  const rows = filter
    ? (stmt.all(`%${filter}%`) as any[])
    : (stmt.all() as any[]);

  return rows.map((row) => ({
    uri: row.uri,
    author: row.author,
    text: row.text,
    timestamp: new Date(row.timestamp).toISOString(),
  }));
}

function getTotalCount(): number {
  return (db.prepare("SELECT COUNT(*) as count FROM posts").get() as any).count;
}

function highlightText(text: string, filter: string): string {
  if (!filter) return escapeHtml(text.slice(0, 280));

  const lowerText = text.toLowerCase();
  const lowerFilter = filter.toLowerCase();
  const matchIndex = lowerText.indexOf(lowerFilter);

  if (matchIndex === -1) return escapeHtml(text.slice(0, 280));

  let start = 0;
  let end = text.length;
  let prefix = "";
  let suffix = "";

  if (matchIndex > 180) {
    start = Math.max(0, matchIndex - 100);
    prefix = "...";
  }

  if (text.length > start + 280) {
    end = start + 280;
    suffix = "...";
  }

  const snippet = text.slice(start, end);
  const escaped = escapeHtml(snippet);
  const filterEscaped = escapeHtml(filter);
  const regex = new RegExp(
    `(${filterEscaped.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")})`,
    "gi",
  );

  return prefix + escaped.replace(regex, "<mark>$1</mark>") + suffix;
}

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function connectToFirehose() {
  const ws = new WebSocket(
    "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
  );

  ws.onopen = () => console.log("🔥 Connected to firehose");

  ws.onmessage = (event) => {
    try {
      if (typeof event.data !== "string") return;
      const message = JSON.parse(event.data);

      if (
        message.kind === "commit" &&
        message.commit?.operation === "create" &&
        message.commit?.collection === "app.bsky.feed.post"
      ) {
        const record = message.commit.record;
        if (record?.text) {
          addPost({
            author: message.did || "unknown",
            text: record.text,
            timestamp: message.time_us
              ? new Date(parseInt(message.time_us) / 1000).toISOString()
              : new Date().toISOString(),
            uri: `at://${message.did}/app.bsky.feed.post/${message.commit.rkey}`,
          });
        }
      }
    } catch {}
  };

  ws.onclose = () => {
    console.log("Firehose disconnected, reconnecting...");
    setTimeout(connectToFirehose, 5000);
  };
}

connectToFirehose();
setInterval(flushPosts, FLUSH_POSTS_MS);
setInterval(cleanupOldPosts, CLEANUP_POSTS_INTERVAL_MS);

function PostItem({ post, filter }: { post: Post; filter: string }) {
  const displayText = highlightText(post.text, filter);
  const author = post.author.split(":").pop()?.slice(0, 16) || "unknown";
  const time = new Date(post.timestamp).toLocaleTimeString();

  return (
    <div class="post">
      <div class="post-author">@{author}</div>
      <div class="post-text">{raw(displayText)}</div>
      <div class="post-meta">{time}</div>
    </div>
  );
}

function Stats({
  filter,
  postsCount,
  totalCount,
}: {
  filter: string;
  postsCount: number;
  totalCount: number;
}) {
  return (
    <div id="stats" class="stats">
      {filter
        ? `Showing ${postsCount} filtered posts (${totalCount} total in last hour)`
        : `Showing ${postsCount} posts from last hour`}
    </div>
  );
}

function Posts({ posts, filter }: { posts: Post[]; filter: string }) {
  return (
    <div id="posts" class="posts">
      {posts.map((post) => (
        <PostItem post={post} filter={filter} />
      ))}
    </div>
  );
}

function FilterInput({ value }: { value: string }) {
  return (
    <div class="filter-section">
      <input
        type="text"
        id="filter"
        placeholder="Filter posts (min 3 chars)..."
        value={value}
        {...{
          "data-bind": "filter",
          "data-on:input__debounce.300ms": "@post('/filter')",
        }}
      />
    </div>
  );
}

function DebugInfo({
  session,
  tab,
  totalCount,
}: {
  session: Session;
  tab: Tab;
  totalCount: number;
}) {
  const allSessions = Array.from(sessions.values()).sort((a, b) =>
    a.id.localeCompare(b.id)
  );
  const allTabs = Array.from(tabs.values()).sort((a, b) =>
    a.id.localeCompare(b.id)
  );

  return (
    <div id="debug" class="debug-section">
      <h2>Stats & Debug</h2>
      <div class="session-info">
        <strong>Session: </strong>
        <span>{session.name}</span>
      </div>
      <div class="session-info">
        <strong>Session ID (client): </strong>
        <span {...{ "data-text": "$sessionId" }}></span>
      </div>
      <div class="session-info">
        <strong>Session ID (server): </strong>
        <span>{session.id}</span>
      </div>
      <div class="session-info">
        <strong>Tab ID: </strong>
        <span {...{ "data-text": "$tabId" }}></span>
      </div>
      <div class="session-info">
        <strong>Filter: </strong>
        <span {...{ "data-text": "$filter" }}></span>
      </div>
      <div class="feed-stats">
        <pre>
          Total posts in DB: {totalCount}
          {"\n"}Retention: 1 hour
        </pre>
      </div>
      <div>
        <strong>Active Sessions: </strong>
        <span class="session-count">{sessions.size}</span>
      </div>
      <ul class="sessions">
        {allSessions.map((s) => (
          <li>{s.name}</li>
        ))}
      </ul>
      <div>
        <strong>Active Tabs: </strong>
        <span class="tab-count">{tabs.size}</span>
      </div>
      <ul class="tabs">
        {allTabs.map((t) => (
          <li>
            Tab {t.id.slice(0, 8)} - {sessions.get(t.sessionId)?.name}
            {t.filter ? ` - filter: "${t.filter}"` : ""}
          </li>
        ))}
      </ul>
    </div>
  );
}

function Styles() {
  return (
    <style>{`
      * {
        box-sizing: border-box;
        margin: 0;
        padding: 0;
      }
      body {
        font-family: system-ui, -apple-system, sans-serif;
        padding: 2rem;
        background: #f5f5f5;
      }
      .container {
        background: white;
        padding: 2rem;
        border-radius: 8px;
      }
      .container-content {
        display: grid;
        grid-template-columns: 2fr 1fr;
        gap: 2rem;
      }
      .main-column {
        min-width: 0;
      }
      .sidebar-column {
        min-width: 0;
      }
      h1 {
        margin-bottom: 1.5rem;
        grid-column: 1 / -1;
      }
      .filter-section {
        margin-bottom: 1.5rem;
        grid-column: 1 / -1;
      }
      input {
        width: 100%;
        padding: 0.75rem;
        font-size: 1rem;
        border: 2px solid #ddd;
        border-radius: 4px;
      }
      .stats {
        margin-bottom: 1rem;
        padding: 0.5rem;
        background: #e3f2fd;
        border-radius: 4px;
      }
      .posts {
        overflow-y: auto;
      }
      .post {
        margin-bottom: 1rem;
        padding: 1rem;
        border: 1px solid #ddd;
        border-radius: 4px;
      }
      .post-author {
        font-weight: bold;
        margin-bottom: 0.5rem;
      }
      .post-text {
        margin-bottom: 0.5rem;
      }
      .post-meta {
        font-size: 0.875rem;
        color: #666;
      }
      mark {
        background: yellow;
      }
      .debug-section h2 {
        margin-bottom: 1rem;
      }
      .debug-section > div {
        margin-bottom: 1rem;
      }
      pre {
        background: #f5f5f5;
        padding: 1rem;
        border-radius: 4px;
        overflow: auto;
        font-size: 0.875rem;
      }
      ul {
        margin-left: 1.5rem;
      }
      li {
        margin-bottom: 0.5rem;
      }
    `}</style>
  );
}

function ContainerContent({ session, tab }: { session: Session; tab: Tab }) {
  const filter = tab.filter.length >= MIN_FILTER_LENGTH ? tab.filter : "";
  const posts = queryPosts(filter);
  const totalCount = getTotalCount();

  return (
    <div id="content" class="container-content">
      <h1>🔥 AT Proto Firehose</h1>
      <FilterInput value={tab.filter} />

      <div class="main-column">
        <Stats
          filter={filter}
          postsCount={posts.length}
          totalCount={totalCount}
        />
        <Posts posts={posts} filter={filter} />
      </div>

      <div class="sidebar-column">
        <DebugInfo session={session} tab={tab} totalCount={totalCount} />
      </div>
    </div>
  );
}

function Container({ session, tab }: { session: Session; tab: Tab }) {
  return (
    <div id="container" class="container">
      <ContainerContent session={session} tab={tab} />
    </div>
  );
}

function Body({ session, tab }: { session: Session; tab: Tab }) {
  return (
    <body
      {...{
        "data-signals": `{filter: '', sessionId: '${session.id}', tabId: '${tab.id}'}`,
        "data-init":
          "$tabId = sessionStorage.getItem('tabId') || $tabId; document.cookie = 'sessionId=' + $sessionId + '; path=/; max-age=31536000'; sessionStorage.setItem('tabId', $tabId); @get('/')",
      }}
    >
      <datastar-inspector></datastar-inspector>
      <Container session={session} tab={tab} />
    </body>
  );
}

function Page({ session, tab }: { session: Session; tab: Tab }) {
  return (
    <html>
      <head>
        <script
          type="module"
          src="https://cdn.jsdelivr.net/gh/starfederation/datastar@1.0.0-RC.6/bundles/datastar.js"
        ></script>
        <Styles />
      </head>
      <Body session={session} tab={tab} />
    </html>
  );
}

const app = new Hono();

app.get("/", async (c) => {
  let sessionId: string | undefined;
  let tabId: string | undefined;

  const isDatastarRequest = c.req.header("datastar-request");
  if (isDatastarRequest) {
    const datastarParam = c.req.query("datastar");
    if (datastarParam) {
      try {
        const signals = JSON.parse(datastarParam);
        sessionId = signals.sessionId;
        tabId = signals.tabId && signals.tabId !== "" ? signals.tabId : undefined;
      } catch {}
    }
  } else {
    const cookie = c.req.header("cookie");
    if (cookie) {
      const match = cookie.match(/sessionId=([^;]+)/);
      if (match) {
        sessionId = match[1];
      }
    }
  }

  const session = getOrCreateSession(sessionId);
  
  if (isDatastarRequest) {
    const tab = getOrCreateTab(tabId, session.id);
    return streamSSE(c, async (stream) => {
      const ac = new AbortController();

      stream.onAbort(() => ac.abort());

      const sendUpdate = async () => {
        const filter = tab.filter.length >= MIN_FILTER_LENGTH ? tab.filter : "";
        const posts = queryPosts(filter);
        const totalCount = getTotalCount();

        const postsHtml = (<Posts posts={posts} filter={filter} />)
          .toString()
          .replaceAll("\n", "");
        const statsHtml = (
          <Stats
            filter={filter}
            postsCount={posts.length}
            totalCount={totalCount}
          />
        )
          .toString()
          .replaceAll("\n", "");
        const debugHtml = (
          <DebugInfo session={session} tab={tab} totalCount={totalCount} />
        )
          .toString()
          .replaceAll("\n", "");

        await stream.writeSSE({
          event: "datastar-patch-elements",
          data: `elements ${postsHtml} ${statsHtml} ${debugHtml}`,
        });
      };

      try {
        const updateQueue = new EventEmitter();

        const postsListener = () => updateQueue.emit("update");
        const filterListener = (updatedTabId: string) => {
          if (updatedTabId === tab.id) {
            updateQueue.emit("update");
          }
        };

        events.on("posts.added", postsListener);
        events.on("filter.updated", filterListener);

        ac.signal.addEventListener("abort", () => {
          events.off("posts.added", postsListener);
          events.off("filter.updated", filterListener);
          tabs.delete(tab.id);
        });

        for await (const _ of on(updateQueue as any, "update", {
          signal: ac.signal,
        })) {
          await sendUpdate();
        }
      } catch (err: any) {
        if (err.code !== "ABORT_ERR") {
          throw err;
        }
      }
    });
  }

  const tab = { id: crypto.randomUUID(), sessionId: session.id, filter: "" };
  return c.html(Page({ session, tab }));
});

app.post("/filter", async (c) => {
  const body = (await c.req.json()) as any;
  const tabId = body.tabId;

  if (!tabId) {
    return c.json({ error: "Missing tab ID" }, 400);
  }

  const tab = tabs.get(tabId);
  if (!tab) {
    return c.json({ error: "Tab not found" }, 404);
  }

  tab.filter = body.filter || "";

  events.emit("filter.updated", tab.id);

  return c.body(null);
});

export default {
  port: 3000,
  fetch: app.fetch,
};

console.log("🔥 AT Proto Firehose running on http://localhost:3000");
