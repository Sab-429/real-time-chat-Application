package cache

// Every Redis key and channel name used by the system lives here, so a typo can
// never make two components disagree and key usage can be audited in one place.
//
//	conn:{userID}    ZSET   member = gatewayID, score = expiry (unix s)   presence + routing
//	gw:{gatewayID}   PUBSUB one channel per gateway                       live delivery
//	rl:{key}         HASH   token bucket state (tokens, ts), TTL'd        rate limiting
//
// Redis holds only ephemeral state. Nothing here is a source of truth.

func ConnKey(userID string) string           { return "conn:" + userID }
func GatewayChannel(gatewayID string) string { return "gw:" + gatewayID }
func RateLimitKey(key string) string         { return "rl:" + key }
