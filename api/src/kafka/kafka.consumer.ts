import {
  Injectable,
  OnModuleInit,
  OnModuleDestroy,
  BeforeApplicationShutdown,
  Logger,
} from '@nestjs/common';
import {
  Kafka,
  Consumer,
  Producer,
  Admin,
  EachMessagePayload,
} from 'kafkajs';
import { RedisService } from '../redis/redis.service';
import { ScoresService, ScoreEvent } from '../scores/scores.service';

@Injectable()
export class KafkaConsumer
  implements OnModuleInit, OnModuleDestroy, BeforeApplicationShutdown
{
  private readonly logger = new Logger(KafkaConsumer.name);
  private readonly kafka: Kafka;
  private readonly sourceTopic = 'score-events';
  private readonly dlqTopic = process.env.KAFKA_DLQ_TOPIC ?? 'score-events-dlq';
  private readonly maxRetries = parseInt(
    process.env.KAFKA_CONSUMER_MAX_RETRIES ?? '3',
    10,
  );

  private consumer: Consumer;
  private producer: Producer;
  private admin: Admin;
  private isShuttingDown = false;
  private shutdownPromise: Promise<void> | null = null;

  constructor(
    private readonly redisService: RedisService,
    private readonly scoresService: ScoresService,
  ) {
    this.kafka = new Kafka({
      clientId: 'scoreboard-api',
      brokers: (process.env.KAFKA_BROKERS ?? 'kafka:29092').split(','),
      retry: { initialRetryTime: 3000, retries: 10 },
    });

    this.consumer = this.kafka.consumer({ groupId: 'scoreboard-group' });
    this.producer = this.kafka.producer();
    this.admin = this.kafka.admin();
  }

  async onModuleInit() {
    await this.admin.connect();
    await this.ensureDlqTopic();

    await this.producer.connect();
    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: this.sourceTopic,
      fromBeginning: false,
    });

    await this.consumer.run({
      eachMessage: async (payload) => {
        await this.processWithRetries(payload);
      },
    });

    this.logJson('consumer_connected', {
      groupId: 'scoreboard-group',
      sourceTopic: this.sourceTopic,
      dlqTopic: this.dlqTopic,
      maxRetries: this.maxRetries,
    });
  }

  private async ensureDlqTopic(): Promise<void> {
    const topics = await this.admin.listTopics();
    if (topics.includes(this.dlqTopic)) return;

    await this.admin.createTopics({
      topics: [
        {
          topic: this.dlqTopic,
          numPartitions: 1,
          replicationFactor: 1,
        },
      ],
    });

    this.logJson('dlq_topic_created', { topic: this.dlqTopic });
  }

  private async processWithRetries(payload: EachMessagePayload): Promise<void> {
    const { topic, partition, message } = payload;
    if (!message.value) return;

    if (this.isShuttingDown) {
      this.warnJson('skip_processing_during_shutdown', {
        topic,
        partition,
        offset: message.offset,
      });
      return;
    }

    const rawValue = message.value.toString();
    let lastError: unknown;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        const event: ScoreEvent = JSON.parse(rawValue);
        await this.handleEvent(event);
        if (attempt > 1) {
          this.warnJson('message_recovered_after_retry', {
            attempt,
            maxRetries: this.maxRetries,
            topic,
            partition,
            offset: message.offset,
          });
        }
        return;
      } catch (error) {
        lastError = error;
        this.warnJson('message_processing_failed_attempt', {
          attempt,
          maxRetries: this.maxRetries,
          topic,
          partition,
          offset: message.offset,
          error: error instanceof Error ? error.message : 'unknown error',
        });
      }
    }

    await this.publishToDlq(payload, rawValue, lastError);
  }

  private async publishToDlq(
    payload: EachMessagePayload,
    rawValue: string,
    lastError: unknown,
  ): Promise<void> {
    const { topic, partition, message } = payload;
    const dlqPayload = {
      originalTopic: topic,
      partition,
      offset: message.offset,
      key: message.key?.toString() ?? null,
      error: lastError instanceof Error ? lastError.message : 'unknown error',
      failedAt: new Date().toISOString(),
      attempts: this.maxRetries,
      payload: rawValue,
    };

    try {
      await this.producer.send({
        topic: this.dlqTopic,
        messages: [
          {
            key: message.key?.toString() ?? `dlq-${message.offset}`,
            value: JSON.stringify(dlqPayload),
          },
        ],
      });

      this.errorJson('message_moved_to_dlq', {
        dlqTopic: this.dlqTopic,
        originalTopic: topic,
        partition,
        offset: message.offset,
      });
    } catch (dlqError) {
      this.errorJson('dlq_publish_failed', {
        dlqTopic: this.dlqTopic,
        originalTopic: topic,
        partition,
        offset: message.offset,
        error: dlqError instanceof Error ? dlqError.message : 'unknown error',
      });
      throw dlqError;
    }
  }

  private async handleEvent(event: ScoreEvent): Promise<void> {
    await this.redisService.setMatchState(event);
    await this.redisService.publishScoreUpdate(event);

    this.scoresService.persistEvent(event).catch((err) => {
      this.errorJson('persist_event_failed_async', {
        matchId: event.matchId,
        error: err instanceof Error ? err.message : 'unknown error',
      });
    });
  }

  async beforeApplicationShutdown(signal?: string) {
    await this.shutdown('before_application_shutdown', signal);
  }

  async onModuleDestroy() {
    await this.shutdown('on_module_destroy');
  }

  private async shutdown(reason: string, signal?: string): Promise<void> {
    if (this.shutdownPromise) return this.shutdownPromise;

    this.isShuttingDown = true;
    this.logJson('consumer_shutdown_start', { reason, signal: signal ?? null });

    // ADR: stop() drains in-flight eachMessage handlers before disconnect, allowing
    // already-processed offsets to be committed by KafkaJS auto-commit.
    this.shutdownPromise = (async () => {
      await this.consumer.stop().catch(() => undefined);
      await Promise.all([
        this.consumer.disconnect().catch(() => undefined),
        this.producer.disconnect().catch(() => undefined),
        this.admin.disconnect().catch(() => undefined),
      ]);
      this.logJson('consumer_shutdown_complete', {
        reason,
        signal: signal ?? null,
      });
    })();

    return this.shutdownPromise;
  }

  private logJson(event: string, data: Record<string, unknown> = {}): void {
    this.logger.log(
      JSON.stringify({
        event,
        ts: new Date().toISOString(),
        ...data,
      }),
    );
  }

  private warnJson(event: string, data: Record<string, unknown> = {}): void {
    this.logger.warn(
      JSON.stringify({
        event,
        ts: new Date().toISOString(),
        ...data,
      }),
    );
  }

  private errorJson(event: string, data: Record<string, unknown> = {}): void {
    this.logger.error(
      JSON.stringify({
        event,
        ts: new Date().toISOString(),
        ...data,
      }),
    );
  }
}
