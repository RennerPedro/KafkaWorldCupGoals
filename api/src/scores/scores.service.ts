import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Match } from '../database/match.entity';
import { ScoreEventEntity } from '../database/score-event.entity';
import { RedisService } from '../redis/redis.service';

export interface ScoreEvent {
  matchId: string;
  homeTeam: string;
  awayTeam: string;
  homeScore: number;
  awayScore: number;
  minute: number;
  timestamp: string;
  source: 'real' | 'simulated';
}

@Injectable()
export class ScoresService {
  private readonly logger = new Logger(ScoresService.name);

  constructor(
    private readonly redisService: RedisService,
    @InjectRepository(Match) private readonly matchRepo: Repository<Match>,
    @InjectRepository(ScoreEventEntity)
    private readonly eventRepo: Repository<ScoreEventEntity>,
  ) {}

  getAllMatches() {
    return this.redisService.getAllMatches();
  }

  getMatch(matchId: string) {
    return this.redisService.getMatchState(matchId);
  }

  getMatchHistory(matchId: string) {
    return this.eventRepo.find({
      where: { match: { id: matchId } },
      order: { minute: 'ASC' },
    });
  }

  async persistEvent(event: ScoreEvent): Promise<void> {
    let match = await this.matchRepo.findOne({ where: { id: event.matchId } });

    if (!match) {
      match = this.matchRepo.create({
        id: event.matchId,
        homeTeam: event.homeTeam,
        awayTeam: event.awayTeam,
      });
      await this.matchRepo.save(match);
    }

    const scoreEvent = this.eventRepo.create({
      match,
      homeScore: event.homeScore,
      awayScore: event.awayScore,
      minute: event.minute,
      source: event.source,
    });
    await this.eventRepo.save(scoreEvent);

    if (event.minute >= 90) {
      await this.matchRepo.update(match.id, { finishedAt: new Date() });
      this.logger.log(`Match ${event.matchId} finished`);
    }
  }
}
