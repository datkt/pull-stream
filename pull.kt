package datkt.pullstream

typealias Callback = (Any?, Any?) -> Any?
typealias Source = (Any?, Callback) -> Any?
typealias Through = (Source) -> Source
typealias Sink = (Source) -> Any?
typealias Partial = Function<*>

interface Duplex {
  val source: Source
  val sink: Sink
  operator fun component1() = source
  operator fun component2() = sink
}

private fun prepend(initial: Any, args: Array<out Any?>): Array<out Any?> {
  return arrayOf(initial, *args)
}

// Through + Sink
fun pull(initial: Function1<*, *>, vararg args: Any?): Sink? {
  var called = false
  return fun (read: Source): Source? {
    if (called) {
      throw Error("Cannot call partial sink more than once")
    }
    called = true
    return pull(*prepend(read, prepend(initial, args)))
  }
}

fun pull(vararg args: Any?): Source? {
  val length = args.count()
  var read: Source? = null

  if (0 == length) {
    throw Error("Cannot create empty pull stream")
  }

  if (false == args[0] is Duplex && false == args[0] is Function2<*, *, *>) {
    throw Error("Pull streams can only be created from functions")
  }

  if (null == args[0]) {
    throw Error("Source stream cannot be null")
  }

  // pull from duplex
  if (args[0] is Duplex) {
    val duplex = args[0] as Duplex
    read = duplex.source
  } else if (args[0] is Function2<*, *, *>) {
    read = args[0] as Source
  }

  for (i in 1..(length - 1)) {
    if (null != read && args[i] is Function1<*, *>) {
      val stream = args[i] as Through
      read = stream(read)
    } else if (null != read && args[i] is Duplex) {
      val (source, sink) = args[i] as Duplex
      sink(read)
      read = source
    } else {
      read = args[i] as Source
    }
  }

  return read
}
