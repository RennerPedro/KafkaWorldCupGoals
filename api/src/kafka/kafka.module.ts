import { Module } from '@nestjs/common';
import { KafkaConsumer } from './kafka.consumer';
import { ScoresModule } from '../scores/scores.module';

@Module({
  imports: [ScoresModule],
  providers: [KafkaConsumer],
})
export class KafkaModule {}
