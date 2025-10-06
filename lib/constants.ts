export enum StartFrom {
  Oldest = '0',
  Latest = '$'
}

export enum RedisStreamCursors {
  Oldest = '0-0',
  Latest = '>'
}

export class AbortErrorAndCloseRedisConnection extends Error {}
