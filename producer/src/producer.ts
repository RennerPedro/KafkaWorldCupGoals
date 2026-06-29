import { Kafka, Producer, logLevel } from 'kafkajs';

interface ScoreEvent {
  matchId: string;
  homeTeam: string;
  awayTeam: string;
  homeScore: number;
  awayScore: number;
  minute: number;
  timestamp: string; // ISO 8601
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

  const kafka = new Kafka({
    clientId: 'score-producer',
    brokers,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 3000, retries: 10 },
  });

  await ensureTopic(kafka);

  const producer: Producer = kafka.producer();
  await producer.connect();
  console.log('[producer] Connected to Kafka — starting match simulation');

  const tick = async (): Promise<void> => {
    // Pick a random non-finished match
    const active = MATCHES.filter((m) => !state[m.matchId].finished);
    if (!active.length) {
      console.log('[producer] All matches finished');
      await producer.disconnect();
      process.exit(0);
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
    };

    // Partition by matchId key — guarantees ordering per match
    await producer.send({
      topic,
      messages: [{ key: match.matchId, value: JSON.stringify(event) }],
    });

    console.log(
      `[producer] ${match.homeTeam} ${s.homeScore}-${s.awayScore} ${match.awayTeam}` +
        ` | min ${s.minute}${s.finished ? ' FT' : ''}`,
    );
  };

  const loop = (): void => {
    const delay = 2000 + Math.random() * 3000; // 2–5 seconds
    setTimeout(async () => {
      await tick();
      loop();
    }, delay);
  };

  loop();
}

run().catch((err) => {
  console.error('[producer] Fatal error', err);
  process.exit(1);
});
