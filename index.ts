export {
  QueueItem,
  StartFrom,
  processRedisStream
} from './processRedisEvents.js'
export { Stream } from './stream.js'

// OpenTelemetry utilities
export {
  RedisStreamTelemetry,
  getTelemetry,
  configureTelemetry,
  type TelemetryConfig
} from './telemetry.js'
