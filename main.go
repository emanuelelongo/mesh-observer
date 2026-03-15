package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/redis/go-redis/v9"
)

type Config struct {
	MQTTBroker                string
	MQTTClientID              string
	MQTTUsername              string
	MQTTPassword              string
	MQTTTLSInsecureSkipVerify bool
	MQTTTLSCAFile             string
	MQTTTopicFilter           string
	MQTTQoS                   byte
	RedisAddr                 string
	RedisPassword             string
	RedisDB                   int
	RedisEventTTL             time.Duration
	RedisPositionTTL          time.Duration
	MapImageEnabled           bool
	MapImageWidth             int
	MapImageHeight            int
	MapProviderURL            string
	AggregationWindow         time.Duration
	CleanupInterval           time.Duration
	MessagePreviewLength      int
	TelegramBotToken          string
	TelegramChatID            string
	MaxObservationsPerMessage int
}

type TopicInfo struct {
	Root          string
	Region        string
	Version       string
	Kind          string
	Channel       string
	GatewayUserID string
}

type Envelope struct {
	ID        uint64      `json:"id"`
	Channel   int         `json:"channel"`
	From      uint64      `json:"from"`
	To        int64       `json:"to"`
	Sender    string      `json:"sender"`
	Timestamp int64       `json:"timestamp"`
	Type      string      `json:"type"`
	Payload   interface{} `json:"payload"`
}

type Observation struct {
	ReceivedAt     time.Time
	Topic          TopicInfo
	Envelope       Envelope
	FromDisplay    string
	PayloadHash    string
	MessagePreview string
	Latitude       *float64
	Longitude      *float64
	HopStart       *int
	HopLimit       *int
	Hops           *int
}

type AggregateEvent struct {
	Key              string
	FirstSeen        time.Time
	LastSeen         time.Time
	FromNodeID       uint64
	FromDisplay      string
	ToNodeID         int64
	Channel          string
	MessageType      string
	MessagePreview   string
	PayloadHash      string
	Observations     []Observation
	SeenByGateways   map[string]struct{}
	GatewayDisplay   map[string]string
	GatewayPositions map[string]GeoPoint
	GatewayHops      map[string]*int
}

type GeoPoint struct {
	Latitude  float64
	Longitude float64
	UpdatedAt time.Time
}

type positionPoint struct {
	NodeID    string
	Latitude  float64
	Longitude float64
}

type gatewayMeta struct {
	LastSeen int64 `json:"last_seen"`
	Hops     *int  `json:"hops,omitempty"`
}

type Notifier interface {
	NotifyUpdate(ctx context.Context, event AggregateEvent) error
	NotifyFinal(ctx context.Context, event AggregateEvent) error
}

type NoopNotifier struct{}

func (n *NoopNotifier) NotifyUpdate(_ context.Context, event AggregateEvent) error {
	log.Printf("event update key=%s gateways=%d hops=%s spread=%s preview=%q",
		event.Key,
		len(event.SeenByGateways),
		formatGatewayHops(event.GatewayDisplay, event.GatewayHops),
		event.LastSeen.Sub(event.FirstSeen).Round(time.Second),
		event.MessagePreview,
	)
	return nil
}

func (n *NoopNotifier) NotifyFinal(_ context.Context, event AggregateEvent) error {
	log.Printf("event final key=%s gateways=%d hops=%s spread=%s preview=%q",
		event.Key,
		len(event.SeenByGateways),
		formatGatewayHops(event.GatewayDisplay, event.GatewayHops),
		event.LastSeen.Sub(event.FirstSeen).Round(time.Second),
		event.MessagePreview,
	)
	return nil
}

type TelegramNotifier struct {
	botToken       string
	chatID         string
	client         *http.Client
	mapEnabled     bool
	mapWidth       int
	mapHeight      int
	mapProviderURL string
	mu             sync.Mutex
	messageIDs     map[string]int64
}

func NewTelegramNotifier(botToken, chatID string, mapEnabled bool, mapWidth, mapHeight int, mapProviderURL string) *TelegramNotifier {
	return &TelegramNotifier{
		botToken: botToken,
		chatID:   chatID,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		mapEnabled:     mapEnabled,
		mapWidth:       mapWidth,
		mapHeight:      mapHeight,
		mapProviderURL: mapProviderURL,
		messageIDs:     map[string]int64{},
	}
}

func (t *TelegramNotifier) NotifyUpdate(ctx context.Context, event AggregateEvent) error {
	return t.sendOrEdit(ctx, event, false)
}

func (t *TelegramNotifier) NotifyFinal(ctx context.Context, event AggregateEvent) error {
	err := t.sendOrEdit(ctx, event, true)
	if t.mapEnabled {
		mapCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		if mapErr := t.sendMapImage(mapCtx, event); mapErr != nil {
			log.Printf("map image send error key=%s: %v", event.Key, mapErr)
		}
		cancel()
	}
	t.mu.Lock()
	delete(t.messageIDs, event.Key)
	t.mu.Unlock()
	return err
}

func (t *TelegramNotifier) sendOrEdit(ctx context.Context, event AggregateEvent, final bool) error {
	msgID, hasMsgID := t.getMessageID(event.Key)
	_ = final

	gateways := make([]string, 0, len(event.SeenByGateways))
	for gw := range event.SeenByGateways {
		if gw == "" {
			continue
		}
		gateways = append(gateways, gw)
	}
	if len(gateways) == 0 {
		gateways = append(gateways, "n/a")
	}

	message := fmt.Sprintf(
		"📡 Mesh propagation report\n"+
			"From: %s\n"+
			"Msg: %q\n"+
			"Channel: %s\n"+
			"Seen by:\n%s\n"+
			"First seen: %s\n"+
			"Last seen: %s\n"+
			"Spread: %s",
		event.FromDisplay,
		event.MessagePreview,
		event.Channel,
		formatGatewayLines(event.GatewayDisplay, event.GatewayHops, event.GatewayPositions),
		event.FirstSeen.Format(time.RFC3339),
		event.LastSeen.Format(time.RFC3339),
		event.LastSeen.Sub(event.FirstSeen).Round(time.Second),
	)

	form := url.Values{}
	form.Set("text", message)

	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", t.botToken)
	if hasMsgID {
		apiURL = fmt.Sprintf("https://api.telegram.org/bot%s/editMessageText", t.botToken)
		form.Set("chat_id", t.chatID)
		form.Set("message_id", strconv.FormatInt(msgID, 10))
	} else {
		form.Set("chat_id", t.chatID)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("create telegram request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("send telegram request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		bodyText := string(body)
		if strings.Contains(strings.ToLower(bodyText), "message is not modified") {
			return nil
		}
		return fmt.Errorf("telegram returned status %s: %s", resp.Status, bodyText)
	}

	if !hasMsgID {
		var payload struct {
			OK     bool `json:"ok"`
			Result struct {
				MessageID int64 `json:"message_id"`
			} `json:"result"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return fmt.Errorf("decode telegram sendMessage response: %w", err)
		}
		if payload.Result.MessageID == 0 {
			return errors.New("telegram sendMessage returned empty message_id")
		}

		t.setMessageID(event.Key, payload.Result.MessageID)
	}

	return nil
}

func (t *TelegramNotifier) getMessageID(key string) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	v, ok := t.messageIDs[key]
	return v, ok
}

func (t *TelegramNotifier) setMessageID(key string, msgID int64) {
	t.mu.Lock()
	t.messageIDs[key] = msgID
	t.mu.Unlock()
}

func (t *TelegramNotifier) sendMapImage(ctx context.Context, event AggregateEvent) error {
	points := make([]positionPoint, 0, len(event.GatewayPositions))
	for nodeID, gp := range event.GatewayPositions {
		points = append(points, positionPoint{NodeID: nodeID, Latitude: gp.Latitude, Longitude: gp.Longitude})
	}
	if len(points) == 0 {
		return nil
	}

	mapURL := buildStaticMapURL(t.mapProviderURL, points, t.mapWidth, t.mapHeight)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mapURL, nil)
	if err != nil {
		return fmt.Errorf("create map request: %w", err)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch map image: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("map provider status: %s", resp.Status)
	}

	imgBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read map image body: %w", err)
	}

	caption := fmt.Sprintf("🗺️ Nodes with coordinates: %d\n%s", len(points), formatGatewayPositions(event.GatewayDisplay, event.GatewayPositions))
	return t.sendPhoto(ctx, imgBytes, caption)
}

func (t *TelegramNotifier) sendPhoto(ctx context.Context, image []byte, caption string) error {
	apiURL := fmt.Sprintf("https://api.telegram.org/bot%s/sendPhoto", t.botToken)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	if err := writer.WriteField("chat_id", t.chatID); err != nil {
		return fmt.Errorf("write chat_id field: %w", err)
	}
	if caption != "" {
		if err := writer.WriteField("caption", caption); err != nil {
			return fmt.Errorf("write caption field: %w", err)
		}
	}

	part, err := writer.CreateFormFile("photo", "mesh-map.png")
	if err != nil {
		return fmt.Errorf("create photo form file: %w", err)
	}
	if _, err := part.Write(image); err != nil {
		return fmt.Errorf("write photo bytes: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, &body)
	if err != nil {
		return fmt.Errorf("create sendPhoto request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("send sendPhoto request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("telegram sendPhoto status: %s", resp.Status)
	}

	return nil
}

type Aggregator struct {
	redis                     *redis.Client
	window                    time.Duration
	eventTTL                  time.Duration
	positionTTL               time.Duration
	previewLen                int
	notifier                  Notifier
	maxObservationsPerMessage int
}

func NewAggregator(redisClient *redis.Client, window, eventTTL, positionTTL time.Duration, previewLen int, maxObs int, notifier Notifier) *Aggregator {
	return &Aggregator{
		redis:                     redisClient,
		window:                    window,
		eventTTL:                  eventTTL,
		positionTTL:               positionTTL,
		previewLen:                previewLen,
		notifier:                  notifier,
		maxObservationsPerMessage: maxObs,
	}
}

func (a *Aggregator) Add(obs Observation) (AggregateEvent, error) {
	ctx := context.Background()

	if obs.Latitude != nil && obs.Longitude != nil {
		nodeID := formatNode(obs.Envelope.From)
		if err := a.upsertNodePosition(ctx, nodeID, *obs.Latitude, *obs.Longitude, obs.ReceivedAt); err != nil {
			return AggregateEvent{}, fmt.Errorf("upsert node position: %w", err)
		}
	}
	if err := a.upsertNodeName(ctx, formatNode(obs.Envelope.From), obs.FromDisplay); err != nil {
		return AggregateEvent{}, fmt.Errorf("upsert node name: %w", err)
	}

	key := makeAggregationKey(obs)
	eventKey := redisEventKey(key)
	gatewaysKey := redisEventGatewaysKey(key)
	gatewaysMetaKey := redisEventGatewaysMetaKey(key)

	exists, err := a.redis.Exists(ctx, eventKey).Result()
	if err != nil {
		return AggregateEvent{}, fmt.Errorf("redis exists: %w", err)
	}

	if exists == 0 {
		_, err = a.redis.HSet(ctx, eventKey,
			"first_seen", obs.ReceivedAt.Unix(),
			"from", strconv.FormatUint(obs.Envelope.From, 10),
			"from_display", obs.FromDisplay,
			"to", strconv.FormatInt(obs.Envelope.To, 10),
			"channel", obs.Topic.Channel,
			"message_type", obs.Envelope.Type,
			"message_preview", truncate(obs.MessagePreview, a.previewLen),
			"payload_hash", obs.PayloadHash,
		).Result()
		if err != nil {
			return AggregateEvent{}, fmt.Errorf("redis hset initial event: %w", err)
		}
	}

	_, err = a.redis.HSet(ctx, eventKey,
		"last_seen", obs.ReceivedAt.Unix(),
		"message_type", obs.Envelope.Type,
		"channel", obs.Topic.Channel,
	).Result()
	if err != nil {
		return AggregateEvent{}, fmt.Errorf("redis hset update event: %w", err)
	}

	if obs.Topic.GatewayUserID != "" {
		if err := a.redis.SAdd(ctx, gatewaysKey, obs.Topic.GatewayUserID).Err(); err != nil {
			return AggregateEvent{}, fmt.Errorf("redis sadd gateways: %w", err)
		}

		meta := gatewayMeta{
			LastSeen: obs.ReceivedAt.Unix(),
			Hops:     obs.Hops,
		}
		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return AggregateEvent{}, fmt.Errorf("marshal gateway meta: %w", err)
		}
		if err := a.redis.HSet(ctx, gatewaysMetaKey, obs.Topic.GatewayUserID, string(metaJSON)).Err(); err != nil {
			return AggregateEvent{}, fmt.Errorf("redis hset gateways meta: %w", err)
		}
	}

	if err := a.redis.Expire(ctx, eventKey, a.eventTTL).Err(); err != nil {
		return AggregateEvent{}, fmt.Errorf("redis expire event key: %w", err)
	}
	if err := a.redis.Expire(ctx, gatewaysKey, a.eventTTL).Err(); err != nil {
		return AggregateEvent{}, fmt.Errorf("redis expire gateways key: %w", err)
	}
	if err := a.redis.Expire(ctx, gatewaysMetaKey, a.eventTTL).Err(); err != nil {
		return AggregateEvent{}, fmt.Errorf("redis expire gateways meta key: %w", err)
	}

	if err := a.redis.ZAdd(ctx, redisEventsLastSeenIndexKey(), redis.Z{
		Score:  float64(obs.ReceivedAt.Unix()),
		Member: key,
	}).Err(); err != nil {
		return AggregateEvent{}, fmt.Errorf("redis zadd last seen index: %w", err)
	}

	event, found, err := a.loadEvent(ctx, key)
	if err != nil {
		return AggregateEvent{}, err
	}
	if !found {
		return AggregateEvent{}, errors.New("event not found after upsert")
	}

	return event, nil
}

func (a *Aggregator) FlushExpired(ctx context.Context, now time.Time) {
	expireBefore := now.Add(-a.window).Unix()
	expiredKeys, err := a.redis.ZRangeByScore(ctx, redisEventsLastSeenIndexKey(), &redis.ZRangeBy{
		Min: "-inf",
		Max: strconv.FormatInt(expireBefore, 10),
	}).Result()
	if err != nil {
		log.Printf("flush expired zrange error: %v", err)
		return
	}

	for _, key := range expiredKeys {
		event, found, err := a.loadEvent(ctx, key)
		if err != nil {
			log.Printf("load expired event key=%s error: %v", key, err)
			continue
		}
		if !found {
			_ = a.redis.ZRem(ctx, redisEventsLastSeenIndexKey(), key).Err()
			continue
		}

		err = a.notifier.NotifyFinal(ctx, event)
		if err != nil {
			log.Printf("final notify error for key=%s: %v", key, err)
		}

		if err := a.deleteEvent(ctx, key); err != nil {
			log.Printf("delete expired event key=%s error: %v", key, err)
		}
	}
}

func (a *Aggregator) FlushAll(ctx context.Context) {
	keys, err := a.redis.ZRange(ctx, redisEventsLastSeenIndexKey(), 0, -1).Result()
	if err != nil {
		log.Printf("flush all zrange error: %v", err)
		return
	}

	for _, key := range keys {
		event, found, err := a.loadEvent(ctx, key)
		if err != nil {
			log.Printf("load event key=%s error: %v", key, err)
			continue
		}
		if !found {
			continue
		}

		err = a.notifier.NotifyFinal(ctx, event)
		if err != nil {
			log.Printf("final notify error for key=%s: %v", key, err)
		}

		if err := a.deleteEvent(ctx, key); err != nil {
			log.Printf("delete event key=%s error: %v", key, err)
		}
	}
}

func (a *Aggregator) upsertNodePosition(ctx context.Context, nodeID string, lat, lon float64, ts time.Time) error {
	key := redisNodePositionKey(nodeID)
	_, err := a.redis.HSet(ctx, key,
		"lat", strconv.FormatFloat(lat, 'f', 7, 64),
		"lon", strconv.FormatFloat(lon, 'f', 7, 64),
		"updated_at", ts.Unix(),
	).Result()
	if err != nil {
		return err
	}
	return a.redis.Expire(ctx, key, a.positionTTL).Err()
}

func (a *Aggregator) loadEvent(ctx context.Context, key string) (AggregateEvent, bool, error) {
	eventKey := redisEventKey(key)
	values, err := a.redis.HGetAll(ctx, eventKey).Result()
	if err != nil {
		return AggregateEvent{}, false, fmt.Errorf("redis hgetall event: %w", err)
	}
	if len(values) == 0 {
		return AggregateEvent{}, false, nil
	}

	firstSeenUnix, _ := strconv.ParseInt(values["first_seen"], 10, 64)
	lastSeenUnix, _ := strconv.ParseInt(values["last_seen"], 10, 64)
	fromNodeID, _ := strconv.ParseUint(values["from"], 10, 64)
	toNodeID, _ := strconv.ParseInt(values["to"], 10, 64)

	gateways, err := a.redis.SMembers(ctx, redisEventGatewaysKey(key)).Result()
	if err != nil {
		return AggregateEvent{}, false, fmt.Errorf("redis smembers gateways: %w", err)
	}
	metaByGateway, err := a.redis.HGetAll(ctx, redisEventGatewaysMetaKey(key)).Result()
	if err != nil {
		return AggregateEvent{}, false, fmt.Errorf("redis hgetall gateways meta: %w", err)
	}

	seenBy := make(map[string]struct{}, len(gateways))
	gatewayDisplay := make(map[string]string, len(gateways))
	positions := make(map[string]GeoPoint)
	hopsByGateway := make(map[string]*int)
	for _, gw := range gateways {
		seenBy[gw] = struct{}{}
		name, err := a.loadNodeName(ctx, gw)
		if err != nil {
			return AggregateEvent{}, false, err
		}
		gatewayDisplay[gw] = fallbackNodeDisplay(name, gw)
		if rawMeta, ok := metaByGateway[gw]; ok && rawMeta != "" {
			var gm gatewayMeta
			if err := json.Unmarshal([]byte(rawMeta), &gm); err == nil {
				hopsByGateway[gw] = gm.Hops
			}
		}
		pos, ok, err := a.loadNodePosition(ctx, gw)
		if err != nil {
			return AggregateEvent{}, false, err
		}
		if ok {
			positions[gw] = pos
		}
	}

	event := AggregateEvent{
		Key:              key,
		FirstSeen:        time.Unix(firstSeenUnix, 0),
		LastSeen:         time.Unix(lastSeenUnix, 0),
		FromNodeID:       fromNodeID,
		FromDisplay:      fallbackDisplayName(values["from_display"], fromNodeID),
		ToNodeID:         toNodeID,
		Channel:          values["channel"],
		MessageType:      values["message_type"],
		MessagePreview:   values["message_preview"],
		PayloadHash:      values["payload_hash"],
		SeenByGateways:   seenBy,
		GatewayDisplay:   gatewayDisplay,
		GatewayPositions: positions,
		GatewayHops:      hopsByGateway,
	}

	return event, true, nil
}

func (a *Aggregator) loadNodePosition(ctx context.Context, nodeID string) (GeoPoint, bool, error) {
	values, err := a.redis.HGetAll(ctx, redisNodePositionKey(nodeID)).Result()
	if err != nil {
		return GeoPoint{}, false, fmt.Errorf("redis hgetall node position: %w", err)
	}
	if len(values) == 0 {
		return GeoPoint{}, false, nil
	}

	lat, err := strconv.ParseFloat(values["lat"], 64)
	if err != nil {
		return GeoPoint{}, false, nil
	}
	lon, err := strconv.ParseFloat(values["lon"], 64)
	if err != nil {
		return GeoPoint{}, false, nil
	}
	updatedAtUnix, _ := strconv.ParseInt(values["updated_at"], 10, 64)

	return GeoPoint{Latitude: lat, Longitude: lon, UpdatedAt: time.Unix(updatedAtUnix, 0)}, true, nil
}

func (a *Aggregator) upsertNodeName(ctx context.Context, nodeID, displayName string) error {
	displayName = strings.TrimSpace(displayName)
	if displayName == "" {
		return nil
	}
	return a.redis.Set(ctx, redisNodeNameKey(nodeID), displayName, a.positionTTL).Err()
}

func (a *Aggregator) loadNodeName(ctx context.Context, nodeID string) (string, error) {
	name, err := a.redis.Get(ctx, redisNodeNameKey(nodeID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("redis get node name: %w", err)
	}
	return name, nil
}

func (a *Aggregator) deleteEvent(ctx context.Context, key string) error {
	pipe := a.redis.Pipeline()
	pipe.Del(ctx, redisEventKey(key))
	pipe.Del(ctx, redisEventGatewaysKey(key))
	pipe.Del(ctx, redisEventGatewaysMetaKey(key))
	pipe.ZRem(ctx, redisEventsLastSeenIndexKey(), key)
	_, err := pipe.Exec(ctx)
	return err
}

func loadConfig() (Config, error) {
	broker := os.Getenv("MQTT_BROKER")
	if broker == "" {
		return Config{}, errors.New("MQTT_BROKER is required, example: tcp://localhost:1883")
	}

	qos := envInt("MQTT_QOS", 1)
	if qos < 0 || qos > 2 {
		return Config{}, fmt.Errorf("MQTT_QOS must be 0, 1 or 2, got %d", qos)
	}

	cfg := Config{
		MQTTBroker:                broker,
		MQTTClientID:              envString("MQTT_CLIENT_ID", "mesh-observer"),
		MQTTUsername:              os.Getenv("MQTT_USERNAME"),
		MQTTPassword:              os.Getenv("MQTT_PASSWORD"),
		MQTTTLSInsecureSkipVerify: envBool("MQTT_TLS_INSECURE_SKIP_VERIFY", false),
		MQTTTLSCAFile:             os.Getenv("MQTT_TLS_CA_FILE"),
		MQTTTopicFilter:           envString("MQTT_TOPIC_FILTER", "msh/+/2/json/#"),
		MQTTQoS:                   byte(qos),
		RedisAddr:                 envString("REDIS_ADDR", "localhost:6379"),
		RedisPassword:             os.Getenv("REDIS_PASSWORD"),
		RedisDB:                   envInt("REDIS_DB", 0),
		RedisEventTTL:             time.Duration(envInt("REDIS_EVENT_TTL_SECONDS", 300)) * time.Second,
		RedisPositionTTL:          time.Duration(envInt("REDIS_POSITION_TTL_SECONDS", 86400)) * time.Second,
		MapImageEnabled:           envBool("MAP_IMAGE_ENABLED", false),
		MapImageWidth:             envInt("MAP_IMAGE_WIDTH", 1024),
		MapImageHeight:            envInt("MAP_IMAGE_HEIGHT", 768),
		MapProviderURL:            envString("MAP_PROVIDER_URL", "https://staticmap.openstreetmap.de/staticmap.php"),
		AggregationWindow:         time.Duration(envInt("AGGREGATION_WINDOW_SECONDS", 60)) * time.Second,
		CleanupInterval:           time.Duration(envInt("CLEANUP_INTERVAL_SECONDS", 5)) * time.Second,
		MessagePreviewLength:      envInt("MESSAGE_PREVIEW_LEN", 80),
		TelegramBotToken:          os.Getenv("TELEGRAM_BOT_TOKEN"),
		TelegramChatID:            os.Getenv("TELEGRAM_CHAT_ID"),
		MaxObservationsPerMessage: envInt("MAX_OBSERVATIONS_PER_MESSAGE", 100),
	}

	if cfg.AggregationWindow <= 0 {
		return Config{}, errors.New("AGGREGATION_WINDOW_SECONDS must be > 0")
	}
	if cfg.CleanupInterval <= 0 {
		return Config{}, errors.New("CLEANUP_INTERVAL_SECONDS must be > 0")
	}
	if cfg.MessagePreviewLength <= 0 {
		return Config{}, errors.New("MESSAGE_PREVIEW_LEN must be > 0")
	}
	if cfg.MaxObservationsPerMessage <= 0 {
		return Config{}, errors.New("MAX_OBSERVATIONS_PER_MESSAGE must be > 0")
	}
	if cfg.RedisAddr == "" {
		return Config{}, errors.New("REDIS_ADDR must not be empty")
	}
	if cfg.RedisEventTTL <= 0 {
		return Config{}, errors.New("REDIS_EVENT_TTL_SECONDS must be > 0")
	}
	if cfg.RedisPositionTTL <= 0 {
		return Config{}, errors.New("REDIS_POSITION_TTL_SECONDS must be > 0")
	}
	if cfg.MapImageWidth <= 0 || cfg.MapImageHeight <= 0 {
		return Config{}, errors.New("MAP_IMAGE_WIDTH and MAP_IMAGE_HEIGHT must be > 0")
	}

	return cfg, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	var notifier Notifier = &NoopNotifier{}
	if cfg.TelegramBotToken != "" && cfg.TelegramChatID != "" {
		notifier = NewTelegramNotifier(
			cfg.TelegramBotToken,
			cfg.TelegramChatID,
			cfg.MapImageEnabled,
			cfg.MapImageWidth,
			cfg.MapImageHeight,
			cfg.MapProviderURL,
		)
		log.Printf("telegram notifier enabled")
	} else {
		log.Printf("telegram notifier disabled (missing TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID)")
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := redisClient.Ping(pingCtx).Err(); err != nil {
		pingCancel()
		log.Fatalf("redis connect error: %v", err)
	}
	pingCancel()
	log.Printf("connected to redis addr=%s db=%d", cfg.RedisAddr, cfg.RedisDB)

	aggregator := NewAggregator(redisClient, cfg.AggregationWindow, cfg.RedisEventTTL, cfg.RedisPositionTTL, cfg.MessagePreviewLength, cfg.MaxObservationsPerMessage, notifier)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(cfg.MQTTBroker)
	opts.SetClientID(cfg.MQTTClientID)
	opts.SetAutoReconnect(true)
	opts.SetConnectRetry(true)
	opts.SetConnectRetryInterval(5 * time.Second)
	opts.SetOrderMatters(false)

	if cfg.MQTTUsername != "" {
		opts.SetUsername(cfg.MQTTUsername)
		opts.SetPassword(cfg.MQTTPassword)
	}

	brokerLower := strings.ToLower(cfg.MQTTBroker)
	if strings.HasPrefix(brokerLower, "ssl://") || strings.HasPrefix(brokerLower, "tls://") {
		tlsConfig := &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: cfg.MQTTTLSInsecureSkipVerify,
		}

		if cfg.MQTTTLSCAFile != "" {
			caBytes, err := os.ReadFile(cfg.MQTTTLSCAFile)
			if err != nil {
				log.Fatalf("mqtt tls ca file read error: %v", err)
			}
			pool := x509.NewCertPool()
			if ok := pool.AppendCertsFromPEM(caBytes); !ok {
				log.Fatalf("mqtt tls ca file parse error")
			}
			tlsConfig.RootCAs = pool
		}

		opts.SetTLSConfig(tlsConfig)
	}

	opts.SetOnConnectHandler(func(client mqtt.Client) {
		log.Printf("connected to mqtt broker=%s, subscribing topic=%s qos=%d", cfg.MQTTBroker, cfg.MQTTTopicFilter, cfg.MQTTQoS)
		token := client.Subscribe(cfg.MQTTTopicFilter, cfg.MQTTQoS, func(_ mqtt.Client, msg mqtt.Message) {
			obs, err := parseObservation(msg.Topic(), msg.Payload(), time.Now(), cfg.MessagePreviewLength)
			if err != nil {
				log.Printf("ignore message topic=%s reason=%v", msg.Topic(), err)
				return
			}

			event, err := aggregator.Add(obs)
			if err != nil {
				log.Printf("aggregator add error topic=%s: %v", msg.Topic(), err)
				return
			}
			updateCtx, updateCancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = notifier.NotifyUpdate(updateCtx, event)
			updateCancel()
			if err != nil {
				log.Printf("update notify error for key=%s: %v", event.Key, err)
			}
		})

		if ok := token.WaitTimeout(15 * time.Second); !ok {
			log.Printf("mqtt subscribe timeout")
			return
		}
		if err := token.Error(); err != nil {
			log.Printf("mqtt subscribe error: %v", err)
			return
		}
		log.Printf("mqtt subscribe ok")
	})

	opts.SetConnectionLostHandler(func(_ mqtt.Client, err error) {
		log.Printf("mqtt connection lost: %v", err)
	})

	client := mqtt.NewClient(opts)
	connectToken := client.Connect()
	if ok := connectToken.WaitTimeout(20 * time.Second); !ok {
		log.Fatalf("mqtt connect timeout")
	}
	if err := connectToken.Error(); err != nil {
		log.Fatalf("mqtt connect error: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ticker := time.NewTicker(cfg.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("shutdown signal received, flushing pending events")
			flushCtx, flushCancel := context.WithTimeout(context.Background(), 20*time.Second)
			aggregator.FlushAll(flushCtx)
			flushCancel()
			_ = redisClient.Close()
			client.Disconnect(1000)
			log.Printf("shutdown complete")
			return
		case now := <-ticker.C:
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			aggregator.FlushExpired(cleanupCtx, now)
			cleanupCancel()
		}
	}
}

func parseObservation(topic string, payload []byte, receivedAt time.Time, previewLen int) (Observation, error) {
	topicInfo, err := parseTopic(topic)
	if err != nil {
		return Observation{}, fmt.Errorf("parse topic: %w", err)
	}

	if topicInfo.Kind != "json" {
		return Observation{}, fmt.Errorf("unsupported topic kind %q", topicInfo.Kind)
	}

	var env Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return Observation{}, fmt.Errorf("invalid json payload: %w", err)
	}

	var root map[string]interface{}
	_ = json.Unmarshal(payload, &root)

	hash := sha256.Sum256(payload)
	payloadHash := hex.EncodeToString(hash[:])

	preview := extractPreview(env)
	preview = truncate(preview, previewLen)

	lat, lon := extractCoordinates(env.Payload)
	hopStart, hopLimit, hops := extractHopFields(root)

	return Observation{
		ReceivedAt:     receivedAt,
		Topic:          topicInfo,
		Envelope:       env,
		FromDisplay:    senderDisplay(env),
		PayloadHash:    payloadHash,
		MessagePreview: preview,
		Latitude:       lat,
		Longitude:      lon,
		HopStart:       hopStart,
		HopLimit:       hopLimit,
		Hops:           hops,
	}, nil
}

func parseTopic(topic string) (TopicInfo, error) {
	parts := strings.Split(topic, "/")
	if len(parts) < 5 {
		return TopicInfo{}, fmt.Errorf("expected at least 5 segments, got %d", len(parts))
	}

	info := TopicInfo{
		Root:    parts[0],
		Region:  parts[1],
		Version: parts[2],
		Kind:    parts[3],
		Channel: parts[4],
	}

	if len(parts) >= 6 {
		info.GatewayUserID = parts[5]
	}

	if info.Root == "" || info.Region == "" {
		return TopicInfo{}, errors.New("invalid root or region")
	}

	return info, nil
}

func makeAggregationKey(obs Observation) string {
	if obs.Envelope.ID != 0 {
		return fmt.Sprintf("%s:%d:%d:%d", obs.Topic.Channel, obs.Envelope.From, obs.Envelope.To, obs.Envelope.ID)
	}

	bucket := obs.ReceivedAt.Unix() / 10
	return fmt.Sprintf("%s:%d:%d:%s:%d", obs.Topic.Channel, obs.Envelope.From, obs.Envelope.To, obs.PayloadHash, bucket)
}

func extractPreview(env Envelope) string {
	if env.Payload == nil {
		return ""
	}

	switch v := env.Payload.(type) {
	case string:
		return v
	case map[string]interface{}:
		if env.Type == "text" {
			if msg, ok := v["text"].(string); ok {
				return msg
			}
		}
		data, err := json.Marshal(v)
		if err != nil {
			return env.Type
		}
		return string(data)
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return env.Type
		}
		return string(data)
	}
}

func extractCoordinates(payload interface{}) (*float64, *float64) {
	obj, ok := payload.(map[string]interface{})
	if !ok {
		return nil, nil
	}

	lat, latOK := readFloat(obj, "latitude")
	lon, lonOK := readFloat(obj, "longitude")
	if latOK && lonOK && validCoord(lat, lon) {
		return &lat, &lon
	}

	latI, latIOK := readFloat(obj, "latitude_i")
	lonI, lonIOK := readFloat(obj, "longitude_i")
	if latIOK && lonIOK {
		lat = latI / 1e7
		lon = lonI / 1e7
		if validCoord(lat, lon) {
			return &lat, &lon
		}
	}

	return nil, nil
}

func readFloat(m map[string]interface{}, key string) (float64, bool) {
	raw, ok := m[key]
	if !ok || raw == nil {
		return 0, false
	}

	switch v := raw.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		if err == nil {
			return f, true
		}
	}

	return 0, false
}

func extractHopFields(root map[string]interface{}) (*int, *int, *int) {
	if len(root) == 0 {
		return nil, nil, nil
	}

	hopStart, hopStartOK := readIntAny(root, "hopStart", "hop_start", "hopstart")
	hopLimit, hopLimitOK := readIntAny(root, "hopLimit", "hop_limit", "hoplimit")

	if payload, ok := root["payload"].(map[string]interface{}); ok {
		if !hopStartOK {
			hopStart, hopStartOK = readIntAny(payload, "hopStart", "hop_start", "hopstart")
		}
		if !hopLimitOK {
			hopLimit, hopLimitOK = readIntAny(payload, "hopLimit", "hop_limit", "hoplimit")
		}
	}

	var hopStartPtr *int
	if hopStartOK {
		h := hopStart
		hopStartPtr = &h
	}
	var hopLimitPtr *int
	if hopLimitOK {
		h := hopLimit
		hopLimitPtr = &h
	}

	if hopStartOK && hopLimitOK && hopStart >= hopLimit {
		hops := hopStart - hopLimit
		return hopStartPtr, hopLimitPtr, &hops
	}

	return hopStartPtr, hopLimitPtr, nil
}

func readIntAny(m map[string]interface{}, keys ...string) (int, bool) {
	for _, k := range keys {
		if v, ok := m[k]; ok && v != nil {
			switch n := v.(type) {
			case float64:
				return int(n), true
			case float32:
				return int(n), true
			case int:
				return n, true
			case int32:
				return int(n), true
			case int64:
				return int(n), true
			case uint32:
				return int(n), true
			case uint64:
				return int(n), true
			case json.Number:
				i, err := n.Int64()
				if err == nil {
					return int(i), true
				}
			}
		}
	}
	return 0, false
}

func validCoord(lat, lon float64) bool {
	if math.IsNaN(lat) || math.IsInf(lat, 0) || math.IsNaN(lon) || math.IsInf(lon, 0) {
		return false
	}
	if lat < -90 || lat > 90 {
		return false
	}
	if lon < -180 || lon > 180 {
		return false
	}
	return true
}

func formatNode(n uint64) string {
	return fmt.Sprintf("!%08x", n)
}

func senderDisplay(env Envelope) string {
	return fallbackDisplayName(env.Sender, env.From)
}

func fallbackDisplayName(name string, nodeID uint64) string {
	name = strings.TrimSpace(name)
	if name != "" {
		return name
	}
	return formatNode(nodeID)
}

func fallbackNodeDisplay(name, nodeID string) string {
	name = strings.TrimSpace(name)
	if name != "" {
		return name
	}
	if nodeID == "" {
		return "n/a"
	}
	return nodeID
}

func formatToNode(to int64) string {
	if to == -1 {
		return "broadcast"
	}
	if to < 0 {
		return strconv.FormatInt(to, 10)
	}
	return fmt.Sprintf("!%08x", uint64(to))
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}

func redisEventKey(key string) string {
	return "event:" + key
}

func redisEventGatewaysKey(key string) string {
	return "event:gateways:" + key
}

func redisEventGatewaysMetaKey(key string) string {
	return "event:gateway_meta:" + key
}

func redisNodePositionKey(nodeID string) string {
	return "node:lastpos:" + nodeID
}

func redisNodeNameKey(nodeID string) string {
	return "node:name:" + nodeID
}

func redisEventsLastSeenIndexKey() string {
	return "events:last_seen"
}

func formatGatewayPositions(displayByGateway map[string]string, positions map[string]GeoPoint) string {
	if len(positions) == 0 {
		return "n/a"
	}

	nodes := make([]string, 0, len(positions))
	for nodeID := range positions {
		nodes = append(nodes, nodeID)
	}
	sort.Strings(nodes)

	parts := make([]string, 0, len(nodes))
	for _, nodeID := range nodes {
		p := positions[nodeID]
		parts = append(parts, fmt.Sprintf("%s(%.5f,%.5f)", fallbackNodeDisplay(displayByGateway[nodeID], nodeID), p.Latitude, p.Longitude))
	}

	return strings.Join(parts, ", ")
}

func formatGatewayHops(displayByGateway map[string]string, hopsByGateway map[string]*int) string {
	if len(hopsByGateway) == 0 {
		return "n/a"
	}

	nodes := make([]string, 0, len(hopsByGateway))
	for nodeID := range hopsByGateway {
		nodes = append(nodes, nodeID)
	}
	sort.Strings(nodes)

	parts := make([]string, 0, len(nodes))
	for _, nodeID := range nodes {
		label := fallbackNodeDisplay(displayByGateway[nodeID], nodeID)
		hops := hopsByGateway[nodeID]
		if hops == nil {
			parts = append(parts, fmt.Sprintf("%s(n/a)", label))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s(%d)", label, *hops))
	}

	return strings.Join(parts, ", ")
}

func formatGatewayLines(displayByGateway map[string]string, hopsByGateway map[string]*int, positions map[string]GeoPoint) string {
	keysSet := map[string]struct{}{}
	for k := range hopsByGateway {
		keysSet[k] = struct{}{}
	}
	for k := range positions {
		keysSet[k] = struct{}{}
	}
	if len(keysSet) == 0 {
		return "n/a"
	}

	keys := make([]string, 0, len(keysSet))
	for k := range keysSet {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	lines := make([]string, 0, len(keys))
	for _, gw := range keys {
		label := fallbackNodeDisplay(displayByGateway[gw], gw)
		hopsText := "n/a"
		if hops, ok := hopsByGateway[gw]; ok && hops != nil {
			hopsText = strconv.Itoa(*hops)
		}

		coordsText := "n/a"
		if p, ok := positions[gw]; ok {
			coordsText = fmt.Sprintf("%.5f, %.5f", p.Latitude, p.Longitude)
		}

		lines = append(lines, fmt.Sprintf("%s(%s hops): %s", label, hopsText, coordsText))
	}

	return strings.Join(lines, "\n")
}

func buildStaticMapURL(base string, points []positionPoint, width, height int) string {
	if len(points) == 0 {
		return base
	}

	var minLat, maxLat, minLon, maxLon float64
	for i, p := range points {
		if i == 0 {
			minLat, maxLat = p.Latitude, p.Latitude
			minLon, maxLon = p.Longitude, p.Longitude
			continue
		}
		if p.Latitude < minLat {
			minLat = p.Latitude
		}
		if p.Latitude > maxLat {
			maxLat = p.Latitude
		}
		if p.Longitude < minLon {
			minLon = p.Longitude
		}
		if p.Longitude > maxLon {
			maxLon = p.Longitude
		}
	}

	centerLat := (minLat + maxLat) / 2
	centerLon := (minLon + maxLon) / 2
	zoom := estimateMapZoom(maxLat-minLat, maxLon-minLon)

	q := url.Values{}
	q.Set("center", fmt.Sprintf("%.6f,%.6f", centerLat, centerLon))
	q.Set("zoom", strconv.Itoa(zoom))
	q.Set("size", fmt.Sprintf("%dx%d", width, height))
	q.Set("maptype", "mapnik")

	markers := make([]string, 0, len(points))
	for _, p := range points {
		markers = append(markers, fmt.Sprintf("%.6f,%.6f,red-pushpin", p.Latitude, p.Longitude))
	}
	q.Set("markers", strings.Join(markers, "|"))

	return strings.TrimRight(base, "?") + "?" + q.Encode()
}

func estimateMapZoom(latSpan, lonSpan float64) int {
	span := math.Max(latSpan, lonSpan)
	switch {
	case span < 0.005:
		return 16
	case span < 0.01:
		return 15
	case span < 0.02:
		return 14
	case span < 0.05:
		return 13
	case span < 0.1:
		return 12
	case span < 0.2:
		return 11
	case span < 0.5:
		return 10
	case span < 1.0:
		return 9
	case span < 2.0:
		return 8
	default:
		return 7
	}
}

func envString(name, fallback string) string {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	return v
}

func envInt(name string, fallback int) int {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func envBool(name string, fallback bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	if v == "" {
		return fallback
	}

	switch v {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}
