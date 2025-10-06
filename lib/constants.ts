export enum StartFrom {
  Oldest = '0',
  Latest = '$'
}

export enum RedisStreamCursors {
  Oldest = '0-0',
  Latest = '>'
}

export type Json =
  | string
  | number
  | boolean
  | null
  | Json[]
  | { [key: string]: Json }
