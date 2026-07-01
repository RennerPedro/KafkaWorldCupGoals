import { Kafka, Producer, logLevel } from 'kafkajs';

interface ScoreEvent {
  matchId: string;
  homeTeam: string;
  awayTeam: string;
  homeScore: number;
  awayScore: number;
  minute: number;
  timestamp: string; // ISO 8601
  source: 'real' | 'simulated';
}

interface WorldCupApiMatch {
  id: number;
  utcDate: string;
  status: string;
  minute?: number | null;
  lastUpdated?: string;
  homeTeam: { name: string };
  awayTeam: { name: string };
  score?: {
    fullTime?: { home?: number | null; away?: number | null };
  };
}

interface WorldCupApiResponse {
  matches?: WorldCupApiMatch[];
}

const MATCHES = [
  { matchId: 'match-001', homeTeam: 'Flamengo', awayTeam: 'Palmeiras' },
  { matchId: 'match-002', homeTeam: 'Barcelona', awayTeam: 'Real Madrid' },
  { matchId: 'match-003', homeTeam: 'Manchester City', awayTeam: 'Arsenal' },
  { matchId: 'match-004', homeTeam: 'Bayern Munich', awayTeam: 'Dortmund' },
  { matchId: 'match-005', homeTeam: 'PSG', awayTeam: 'Marseille' },
];

interface MatchState {
  homeScore: number;
  awayScore: number;
  minute: number;
  finished: boolean;
}

const state: Record<string, MatchState> = {};
MATCHES.forEach((m) => {
  state[m.matchId] = { homeScore: 0, awayScore: 0, minute: 0, finished: false };
});

function isLiveLikeStatus(status: string): boolean {
  return ['LIVE', 'IN_PLAY', 'PAUSED', 'EXTRA_TIME', 'PENALTY_SHOOTOUT'].includes(status);
}

function estimateMinute(match: WorldCupApiMatch): number {
  if (typeof match.minute === 'number') {
    return Math.max(0, Math.min(120, Math.floor(match.minute)));
  }

  if (match.status === 'FINISHED') return 90;
  if (!isLiveLikeStatus(match.status)) return 0;

  const kickoff = new Date(match.utcDate).getTime();
  if (Number.isNaN(kickoff)) return 0;
  const elapsed = Math.floor((Date.now() - kickoff) / 60000);
  return Math.max(0, Math.min(120, elapsed));
}

function mapRealMatchToScoreEvent(match: WorldCupApiMatch): ScoreEvent {
  return {
    matchId: `wc-${match.id}`,
    homeTeam: match.homeTeam?.name ?? 'Unknown Home',
    awayTeam: match.awayTeam?.name ?? 'Unknown Away',
    homeScore: Math.max(0, match.score?.fullTime?.home ?? 0),
    awayScore: Math.max(0, match.score?.fullTime?.away ?? 0),
    minute: estimateMinute(match),
    timestamp: match.lastUpdated ?? new Date().toISOString(),
    source: 'real',
  };
}

async function fetchWorldCupMatches(
  apiBaseUrl: string,
  token: string,
): Promise<WorldCupApiMatch[]> {
  // ADR: Polling is used because public third-party score APIs typically do not
  // provide webhooks for free/public tiers, making pull-based sync the viable option.
  const url = `${apiBaseUrl}/competitions/WC/matches?status=LIVE,IN_PLAY,PAUSED,TIMED,SCHEDULED,FINISHED`;
  const response = await fetch(url, {
    headers: {
      'X-Auth-Token': token,
    },
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`World Cup API error ${response.status}: ${body.slice(0, 200)}`);
  }

  const payload = (await response.json()) as WorldCupApiResponse;
  return payload.matches ?? [];
}

async function publishEvent(producer: Producer, topic: string, event: ScoreEvent): Promise<void> {
  // Partition by matchId key — guarantees ordering per match
  await producer.send({
    topic,
    messages: [{ key: event.matchId, value: JSON.stringify(event) }],
  });
}

async function emitSimulatedTick(producer: Producer, topic: string): Promise<void> {
  // Pick a random non-finished match
  const active = MATCHES.filter((m) => !state[m.matchId].finished);
  if (!active.length) {
    console.log('[producer] All simulated matches finished');
    return;
  }

  const match = active[Math.floor(Math.random() * active.length)];
  const s = state[match.matchId];

  // Advance minute by 1–3 ticks
  s.minute = Math.min(s.minute + Math.floor(Math.random() * 3) + 1, 90);

  // 30% chance of a goal on each tick
  if (Math.random() < 0.3) {
    if (Math.random() < 0.5) s.homeScore++;
    else s.awayScore++;
  }

  if (s.minute >= 90) s.finished = true;

  const event: ScoreEvent = {
    matchId: match.matchId,
    homeTeam: match.homeTeam,
    awayTeam: match.awayTeam,
    homeScore: s.homeScore,
    awayScore: s.awayScore,
    minute: s.minute,
    timestamp: new Date().toISOString(),
    source: 'simulated',
  };

  await publishEvent(producer, topic, event);

  console.log(
    `[producer][simulated] ${match.homeTeam} ${s.homeScore}-${s.awayScore} ${match.awayTeam}` +
      ` | min ${s.minute}${s.finished ? ' FT' : ''}`,
  );
}

async function ensureTopic(kafka: Kafka): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const existing = await admin.listTopics();
    if (!existing.includes('score-events')) {
      await admin.createTopics({
        topics: [
          {
            topic: 'score-events',
            numPartitions: 4,
            replicationFactor: 1,
            configEntries: [
              // 24 hours retention
              { name: 'retention.ms', value: String(24 * 60 * 60 * 1000) },
            ],
          },
        ],
      });
      console.log('[producer] Topic score-events created (4 partitions, 24h retention)');
    } else {
      console.log('[producer] Topic score-events already exists');
    }
  } finally {
    await admin.disconnect();
  }
}

async function run(): Promise<void> {
  const brokers = (process.env.KAFKA_BROKERS ?? 'kafka:29092').split(',');
  const topic = process.env.KAFKA_TOPIC ?? 'score-events';
  const useRealApi = (process.env.USE_REAL_API ?? 'false').toLowerCase() === 'true';
  const apiBaseUrl = (process.env.WORLD_CUP_API_URL ?? 'https://api.football-data.org/v4').replace(
    /\/$/,
    '',
  );
  const apiToken = process.env.WORLD_CUP_API_TOKEN ?? '';
  const pollIntervalMs = parseInt(process.env.WORLD_CUP_POLL_INTERVAL_MS ?? '15000', 10);

  const kafka = new Kafka({
    clientId: 'score-producer',
    brokers,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 3000, retries: 10 },
  });

  await ensureTopic(kafka);

  const producer: Producer = kafka.producer();
  await producer.connect();
  if (useRealApi && !apiToken) {
    console.warn('[producer] USE_REAL_API=true but WORLD_CUP_API_TOKEN is empty, falling back to simulated mode');
  }

  const shouldUseReal = useRealApi && Boolean(apiToken);

  if (shouldUseReal) {
    console.log(`[producer] Connected to Kafka — mode=real (World Cup API), interval=${pollIntervalMs}ms`);

    let inFlight = false;
    const poll = async () => {
      if (inFlight) return;
      inFlight = true;
      try {
        const matches = await fetchWorldCupMatches(apiBaseUrl, apiToken);
        if (!matches.length) {
          console.log('[producer][real] World Cup API returned no matches for current query window');
          return;
        }

        for (const match of matches) {
          const event = mapRealMatchToScoreEvent(match);
          await publishEvent(producer, topic, event);
          console.log(
            `[producer][real] ${event.homeTeam} ${event.homeScore}-${event.awayScore} ${event.awayTeam}` +
              ` | min ${event.minute}`,
          );
        }
      } catch (error) {
        console.error('[producer][real] Polling failed, emitting one simulated tick as fallback', error);
        await emitSimulatedTick(producer, topic);
      } finally {
        inFlight = false;
      }
    };

    await poll();
    setInterval(() => {
      void poll();
    }, Math.max(5000, pollIntervalMs));
    return;
  }

  console.log('[producer] Connected to Kafka — mode=simulated');
  const loop = (): void => {
    const delay = 2000 + Math.random() * 3000; // 2–5 seconds
    setTimeout(async () => {
      await emitSimulatedTick(producer, topic);
      loop();
    }, delay);
  };
  loop();
}

run().catch((err) => {
  console.error('[producer] Fatal error', err);
  process.exit(1);
});
