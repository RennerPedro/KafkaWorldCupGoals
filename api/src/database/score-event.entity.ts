import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  ManyToOne,
  JoinColumn,
} from 'typeorm';
import { Match } from './match.entity';

@Entity('score_events')
export class ScoreEventEntity {
  @PrimaryGeneratedColumn('uuid')
  id: string;

  @ManyToOne(() => Match, (m) => m.events)
  @JoinColumn({ name: 'match_id' })
  match: Match;

  @Column({ name: 'home_score' })
  homeScore: number;

  @Column({ name: 'away_score' })
  awayScore: number;

  @Column()
  minute: number;

  @Column({ type: 'varchar', length: 16, default: 'simulated' })
  source: 'real' | 'simulated';

  @CreateDateColumn({ name: 'recorded_at', type: 'timestamptz' })
  recordedAt: Date;
}
