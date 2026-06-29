import {
  Injectable,
  OnModuleInit,
  OnModuleDestroy,
  Logger,
} from '@nestjs/common';
import { Kafka, Consumer } from 'kafkajs';
import { RedisService } from '../redis/redis.service';
import { ScoresService, ScoreEvent } from '../scores/scores.service';

@Injectable()
export class KafkaConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumer.name);
  private consumer: Consumer;

  constructor(
    private readonly redisService: RedisService,
    private readonly scoresService: ScoresService,
  ) {
    const kafka = new Kafka({
      clientId: 'scoreboard-api',
      brokers: (process.env.KAFKA_BROKERS ?? 'kafka:29092').split(','),
      retry: { initialRetryTime: 3000, retries: 10 },
    });
    this.consumer = kafka.consumer({ groupId: 'scoreboard-group' });
  }

  async onModuleInit() {
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: 'score-events',
      fromBeginning: false,
    });

    await this.consumer.run({
      eachMessage: async ({ message }) => {
        if (!message.value) return;
        const event: ScoreEvent = JSON.parse(message.value.toString());
        await this.handleEvent(event);
      },
    });

    this.logger.log('Kafka consumer connected — group: scoreboard-group');
  }

  private async handleEvent(event: ScoreEvent): Promise<void> {
    // 1. Update current match state in Redis (sub-ms read path for REST/WS)
    await this.redisService.setMatchState(event);

    // 2. Publish to Redis pub/sub so ALL API instances fan-out via WebSocket
    await this.redisService.publishScoreUpdate(event);

    // 3. Persist to PostgreSQL asynchronously — does not block the consumer loop
    this.scoresService.persistEvent(event).catch((err) =>
      this.logger.error(`Failed to persist event for match ${event.matchId}`, err),
    );
  }

  async onModuleDestroy() {
    await this.consumer.disconnect();
  }
}
