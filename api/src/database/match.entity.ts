import {
  Entity,
  PrimaryColumn,
  Column,
  CreateDateColumn,
  OneToMany,
} from 'typeorm';
import { ScoreEventEntity } from './score-event.entity';

@Entity('matches')
export class Match {
  @PrimaryColumn({ length: 100 })
  id: string;

  @Column({ name: 'home_team', length: 100 })
  homeTeam: string;

  @Column({ name: 'away_team', length: 100 })
  awayTeam: string;

  @CreateDateColumn({ name: 'started_at', type: 'timestamptz' })
  startedAt: Date;

  @Column({ name: 'finished_at', type: 'timestamptz', nullable: true })
  finishedAt: Date;

  @OneToMany(() => ScoreEventEntity, (e) => e.match)
  events: ScoreEventEntity[];
}
