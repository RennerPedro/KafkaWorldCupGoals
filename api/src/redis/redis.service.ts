import { Injectable, OnModuleDestroy, Logger } from '@nestjs/common';
import Redis from 'ioredis';

@Injectable()
export class RedisService implements OnModuleDestroy {
  private readonly logger = new Logger(RedisService.name);

  // Three separate connections: state R/W, publisher, subscriber
  // Initialized in the constructor so they are available when the WebSocket
  // gateway's afterInit hook fires (which runs before onModuleInit).
  private readonly state: Redis;
  private readonly publisher: Redis;
  private readonly subscriber: Redis;

  constructor() {
    const url = process.env.REDIS_URL ?? 'redis://redis:6379';
    this.state = new Redis(url);
    this.publisher = new Redis(url);
    this.subscriber = new Redis(url);
    this.logger.log('Redis connections established');
  }

  // ── State store ────────────────────────────────────────────────────────────

  async setMatchState(event: {
    matchId: string;
    homeTeam: string;
    awayTeam: string;
    homeScore: number;
    awayScore: number;
    minute: number;
    timestamp: string;
    source: 'real' | 'simulated';
  }): Promise<void> {
    const { matchId, homeTeam, awayTeam, homeScore, awayScore, minute, timestamp, source } =
      event;
    await this.state.hset(`match:${matchId}`, {
      matchId,
      homeTeam,
      awayTeam,
      homeScore: String(homeScore),
      awayScore: String(awayScore),
      minute: String(minute),
      timestamp,
      source,
    });
  }

  async getMatchState(matchId: string): Promise<Record<string, string> | null> {
    const data = await this.state.hgetall(`match:${matchId}`);
    return Object.keys(data).length ? data : null;
  }

  async getAllMatches(): Promise<Record<string, string>[]> {
    const keys = await this.state.keys('match:*');
    if (!keys.length) return [];
    const pipeline = this.state.pipeline();
    keys.forEach((k) => pipeline.hgetall(k));
    const results = await pipeline.exec();
    return (results ?? []).map(([, v]) => v as Record<string, string>);
  }

  async ping(): Promise<string> {
    return this.state.ping();
  }

  // ── Pub/Sub ────────────────────────────────────────────────────────────────

  async publishScoreUpdate(event: object): Promise<void> {
    await this.publisher.publish('score-updates', JSON.stringify(event));
  }

  subscribeToScoreUpdates(callback: (message: string) => void): void {
    this.subscriber.subscribe('score-updates', (err) => {
      if (err) this.logger.error('Redis subscribe error', err);
    });
    this.subscriber.on('message', (_channel, message) => callback(message));
  }

  // ── Lifecycle ──────────────────────────────────────────────────────────────

  async onModuleDestroy() {
    await Promise.all([
      this.state.quit(),
      this.publisher.quit(),
      this.subscriber.quit(),
    ]);
  }
}
