export function chunk<T>(array: T[], size: number): T[][] {
  const chunks: T[][] = []
  let index = 0

  while (index < array.length) {
    chunks.push(array.slice(index, index + size))
    index += size
  }

  return chunks
}
