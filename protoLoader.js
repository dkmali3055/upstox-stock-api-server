const protobuf = require("protobufjs");

let FeedResponseType = null;

async function loadProto(path = "./MarketDataFeedV3.proto") {
  if (FeedResponseType) return FeedResponseType;
  const root = await protobuf.load(path);
  FeedResponseType = root.lookupType(
    "com.upstox.marketdatafeederv3udapi.rpc.proto.FeedResponse"
  );
  return FeedResponseType;
}

async function decodeMarketFeed(buffer) {
  const FeedResponse = await loadProto();
  const decoded = FeedResponse.decode(buffer);
  return FeedResponse.toObject(decoded, {
    longs: String,
    enums: String,
    defaults: true,
    arrays: true,
  });
}

module.exports = { decodeMarketFeed };
