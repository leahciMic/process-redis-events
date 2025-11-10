type Handler<T> = T extends void ? () => void : (arg: T) => void
type Arg<T> = T extends void ? [] : [T]

export class TypedEmitter<E extends Record<string, unknown>> {
  #listeners: { [K in keyof E]?: Set<Handler<E[K]>> } = {}

  on<K extends keyof E>(type: K, fn: Handler<E[K]>): () => void {
    const set = (this.#listeners[type] ??= new Set())
    set.add(fn as any)
    return () => this.off(type, fn)
  }

  once<K extends keyof E>(type: K, fn: Handler<E[K]>): () => void {
    const off = this.on(type, ((payload: any) => {
      off()
      ;(fn as any)(payload)
    }) as Handler<E[K]>)
    return off
  }

  off<K extends keyof E>(type: K, fn: Handler<E[K]>): void {
    this.#listeners[type]?.delete(fn as any)
  }

  emit<K extends keyof E>(type: K, ...args: Arg<E[K]>): void {
    this.#listeners[type]?.forEach((fn) => (fn as any)(...args))
  }
}
