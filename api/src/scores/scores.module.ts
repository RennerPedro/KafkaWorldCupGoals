import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Match } from '../database/match.entity';
import { ScoreEventEntity } from '../database/score-event.entity';
import { ScoresController } from './scores.controller';
import { ScoresService } from './scores.service';
import { ScoresGateway } from './scores.gateway';

@Module({
  imports: [TypeOrmModule.forFeature([Match, ScoreEventEntity])],
  controllers: [ScoresController],
  providers: [ScoresService, ScoresGateway],
  exports: [ScoresService],
})
export class ScoresModule {}
