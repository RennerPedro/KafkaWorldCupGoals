import {
  WebSocketGateway,
  WebSocketServer,
  OnGatewayInit,
  OnGatewayConnection,
} from '@nestjs/websockets';
import { Logger } from '@nestjs/common';
import { Server, Socket } from 'socket.io';
import { RedisService } from '../redis/redis.service';
import { ScoresService } from './scores.service';

// Architectural note: Redis pub/sub fan-out ensures that clients connected to
// this API instance receive events consumed by ANY instance in the cluster.
// Without this, clients on instance B would miss events consumed by instance A.
@WebSocketGateway({ cors: { origin: '*' } })
export class ScoresGateway implements OnGatewayInit, OnGatewayConnection {
  @WebSocketServer()
  server: Server;

  private readonly logger = new Logger(ScoresGateway.name);

  constructor(
    private readonly redisService: RedisService,
    private readonly scoresService: ScoresService,
  ) {}

  // Called after the WebSocket server is fully initialized — safe to use server here
  afterInit() {
    this.redisService.subscribeToScoreUpdates((message) => {
      this.server.emit('score:updated', JSON.parse(message));
    });
    this.logger.log('WebSocket gateway initialized, subscribed to Redis pub/sub');
  }

  async handleConnection(client: Socket) {
    this.logger.log(`Client connected: ${client.id}`);
    const matches = await this.scoresService.getAllMatches();
    client.emit('matches:snapshot', matches);
  }
}
