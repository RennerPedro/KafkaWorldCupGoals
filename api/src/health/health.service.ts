import {
  Injectable,
  Logger,
  ServiceUnavailableException,
} from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { Kafka } from 'kafkajs';
import { DataSource } from 'typeorm';
import { RedisService } from '../redis/redis.service';

type DependencyStatus = 'up' | 'down';

@Injectable()
export class HealthService {
  private readonly logger = new Logger(HealthService.name);

  constructor(
    private readonly redisService: RedisService,
    @InjectDataSource() private readonly dataSource: DataSource,
  ) {}

  async check() {
    const [redis, postgres, kafka] = await Promise.all([
      this.checkRedis(),
      this.checkPostgres(),
      this.checkKafka(),
    ]);

    const dependencies = { redis, postgres, kafka };
    const isHealthy = Object.values(dependencies).every((dependency) => dependency.status === 'up');
    const payload = {
      service: 'api',
      status: isHealthy ? 'ok' : 'degraded',
      ts: new Date().toISOString(),
      dependencies,
    };

    if (!isHealthy) {
      throw new ServiceUnavailableException(payload);
    }

    return payload;
  }

  private async checkRedis(): Promise<{ status: DependencyStatus; detail?: string }> {
    try {
      const pong = await this.redisService.ping();
      return { status: pong === 'PONG' ? 'up' : 'down', detail: pong };
    } catch (error) {
      this.logger.error('Redis health check failed', error);
      return { status: 'down', detail: error instanceof Error ? error.message : 'unknown redis error' };
    }
  }

  private async checkPostgres(): Promise<{ status: DependencyStatus; detail?: string }> {
    try {
      await this.dataSource.query('SELECT 1');
      return { status: 'up' };
    } catch (error) {
      this.logger.error('Postgres health check failed', error);
      return { status: 'down', detail: error instanceof Error ? error.message : 'unknown postgres error' };
    }
  }

  private async checkKafka(): Promise<{ status: DependencyStatus; detail?: string }> {
    const kafka = new Kafka({
      clientId: 'scoreboard-healthcheck',
      brokers: (process.env.KAFKA_BROKERS ?? 'kafka:29092').split(','),
      retry: { initialRetryTime: 1000, retries: 2 },
    });
    const admin = kafka.admin();

    try {
      await admin.connect();
      await admin.listTopics();
      return { status: 'up' };
    } catch (error) {
      this.logger.error('Kafka health check failed', error);
      return { status: 'down', detail: error instanceof Error ? error.message : 'unknown kafka error' };
    } finally {
      await admin.disconnect().catch(() => undefined);
    }
  }
}