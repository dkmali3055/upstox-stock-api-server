// upstox-v3-ws.js
const axios = require("axios");
const WebSocket = require("ws").WebSocket;
const protobuf = require("protobufjs");
const { v4: uuidv4 } = require("uuid");

// ----------------------------
// CONFIG
// ----------------------------
const ACCESS_TOKEN = ""; // put your token here
const API_URL = "https://api.upstox.com/v3/feed/market-data-feed/authorize";
let protobufRoot = null;

// ----------------------------
// INIT PROTOBUF
// ----------------------------
async function initProtobuf() {
  protobufRoot = await protobuf.load(__dirname + "/MarketDataFeed.proto");
  console.log("✅ Protobuf v3 initialized");
}

// ----------------------------
// FETCH ONE-TIME WS URL
// ----------------------------
async function getMarketFeedUrlV3() {
  try {
    const res = await axios.get(API_URL, {
      headers: {
        Authorization: `Bearer ${ACCESS_TOKEN}`,
        Accept: "application/json",
      },
    });

    return res.data.data.authorized_redirect_uri;
  } catch (err) {
    throw new Error(
      `Auth request failed: ${err.response?.status} ${err.response?.statusText}`
    );
  }
}

// ----------------------------
// DECODE PROTOBUF
// ----------------------------
function decodeProtobufV3(buffer) {
  if (!protobufRoot) {
    console.warn("⚠️ Protobuf not initialized yet");
    return null;
  }
  const FeedResponse = protobufRoot.lookupType(
    "com.upstox.marketdatafeeder.rpc.proto.FeedResponse"
  );
  return FeedResponse.decode(buffer);
}

// ----------------------------
// CONNECT + SUBSCRIBE
// ----------------------------
async function connectWebSocketV3(wsUrl) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl, {
      headers: {
        Authorization: `Bearer ${ACCESS_TOKEN}`,
        Accept: "*/*",
      },
      followRedirects: true,
    });

    ws.on("open", () => {
      console.log("🔌 Connected to v3 WebSocket");

      // subscription message
      const subscribeMsg = {
        guid: uuidv4(),
        method: "sub",
        data: {
          mode: "full", // ltpc | option_greeks | full | full_d30
          instrumentKeys: ["NSE_INDEX|Nifty 50", "NSE_INDEX|ETERNAL"],
        },
      };

      ws.send(Buffer.from(JSON.stringify(subscribeMsg)));
      resolve(ws);
    });

    ws.on("message", (data) => {
      try {
        const decoded = decodeProtobufV3(data);
        console.log("📈 Market Update:", JSON.stringify(decoded));
      } catch (err) {
        console.error("❌ Decode error:", err.message);
      }
    });

    ws.on("close", () => {
      console.log("⚠️ WebSocket disconnected");
      setTimeout(() => reconnect(), 3000);
    });

    ws.on("error", (err) => {
      console.error("❌ WebSocket error:", err.message);
      reject(err);
    });
  });
}

// ----------------------------
// RECONNECT HANDLER
// ----------------------------
async function reconnect() {
  try {
    console.log("♻️ Reconnecting...");
    const wsUrl = await getMarketFeedUrlV3();
    await connectWebSocketV3(wsUrl);
  } catch (err) {
    console.error("Reconnect failed:", err.message);
    setTimeout(reconnect, 5000);
  }
}

// ----------------------------
// MAIN
// ----------------------------
(async () => {
  try {
    await initProtobuf();
    const wsUrl = await getMarketFeedUrlV3();
    console.log("🔗 WebSocket URL:", wsUrl);
    await connectWebSocketV3(wsUrl);
  } catch (err) {
    console.error("Error setting up WebSocket v3:", err.message);
  }
})();
