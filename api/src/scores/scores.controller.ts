import { Controller, Get, Param } from '@nestjs/common';
import { ScoresService } from './scores.service';

@Controller()
export class ScoresController {
  constructor(private readonly scoresService: ScoresService) {}

  @Get('matches')
  getAllMatches() {
    return this.scoresService.getAllMatches();
  }

  @Get('matches/:id')
  getMatch(@Param('id') id: string) {
    return this.scoresService.getMatch(id);
  }

  @Get('matches/:id/history')
  getMatchHistory(@Param('id') id: string) {
    return this.scoresService.getMatchHistory(id);
  }
}
