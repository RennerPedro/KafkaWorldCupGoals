import { NestFactory } from '@nestjs/core';
import { ConsoleLogger } from '@nestjs/common';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule, {
    logger: new ConsoleLogger('LiveScoreAPI', {
      timestamp: true,
    }),
  });
  app.enableShutdownHooks();
  app.enableCors();
  const port = process.env.PORT ?? 3001;
  await app.listen(port);
  const logger = new ConsoleLogger('Bootstrap', {
    timestamp: true,
  });
  logger.log(
    JSON.stringify({
      event: 'api_started',
      port,
    }),
  );
}

bootstrap();
