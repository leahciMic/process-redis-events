export function parseJson<T>(value: unknown): T {
  if (typeof value !== 'string') {
    throw new Error('Value is not a string')
  }
  return JSON.parse(value)
}
