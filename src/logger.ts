import pino from 'pino';
import { config } from './config.js';

const pretty = process.env.NODE_ENV !== 'production' && process.stdout.isTTY;

export const logger = pino({
  level: config.LOG_LEVEL,
  base: undefined,
  ...(pretty
    ? { transport: { target: 'pino-pretty', options: { translateTime: 'HH:MM:ss.l', ignore: 'pid,hostname' } } }
    : {}),
});

export type Logger = typeof logger;
