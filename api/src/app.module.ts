import { Module } from '@nestjs/common';
import { DatabaseModule } from './database/database.module';
import { RedisModule } from './redis/redis.module';
import { ScoresModule } from './scores/scores.module';
import { KafkaModule } from './kafka/kafka.module';

@Module({
  imports: [DatabaseModule, RedisModule, ScoresModule, KafkaModule],
})
export class AppModule {}
