import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Match } from './match.entity';
import { ScoreEventEntity } from './score-event.entity';

@Module({
  imports: [
    TypeOrmModule.forRoot({
      type: 'postgres',
      host: process.env.POSTGRES_HOST ?? 'postgres',
      port: parseInt(process.env.POSTGRES_PORT ?? '5432'),
      database: process.env.POSTGRES_DB ?? 'livescore',
      username: process.env.POSTGRES_USER ?? 'livescore',
      password: process.env.POSTGRES_PASSWORD ?? 'livescore',
      entities: [Match, ScoreEventEntity],
      synchronize: true,
      retryAttempts: 10,
      retryDelay: 3000,
    }),
  ],
  exports: [TypeOrmModule],
})
export class DatabaseModule {}
