package datkt.pullstream.test

import datkt.pullstream.Callback
import datkt.pullstream.Through
import datkt.pullstream.Source
import datkt.pullstream.Sink
import datkt.pullstream.pull

import datkt.tape.test

fun tests.pull(argv: Array<String>) {
  fun <T, R> curry(fn: (Source, T) -> R) = { arg: T ->
    fun(read: Callback) = fn(read, arg)
  }

  fun <T> values(vararg args: T): Source {
    val length = args.count()
    var i = 0
    return { abort, cb ->
      if (null != abort && abort is Boolean && true == abort) {
        cb(true, null)
      } else if (i >= length) {
        cb(true, null)
      } else {
        cb(null, args[i++])
      }
    }
  }

  val map = curry<(Int) -> Int, Source> { read, mapper ->
    { abort, cb ->
      read(abort) { end, value ->
        if (null != end && end is Boolean && true == end) {
          cb(end, null)
        } else {
          cb(null, mapper(value as Int))
        }
      }
    }
  }

  val sum = curry<(Any?, Int) -> Any?, Any?> { read, done ->
    var total = 0

    fun next(end: Any?, value: Any?) {
      if (null != end && end is Boolean && true == end) {
        done(null, total)
      } else {
        total += value as Int
        read(null, ::next)
      }
    }

    read(null, ::next)
  }

  val log = curry<(Any?) -> Any?, Source> { read, logger ->
    { abort, cb ->
      read(abort) { end, value ->
        if (null != end && end is Boolean && true == end) {
          cb(end, null)
        } else {
          logger(value)
          cb(null, value)
        }
      }
    }
  }

  test("pull(vararg args: Function<*>): Function<*>") { t ->
    t.throws({ pull() }, Error::class, "Fails for empty arguments")
    t.throws({ pull(true) }, Error::class, "Fails for in correct arguments")
    t.throws({ pull(null) }, Error::class, "Fails for in correct arguments")
    t.throws({ pull("") }, Error::class, "Fails for in correct arguments")

    t.end()
  }

  test("pull() combines streams") { t ->
    pull(
      values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      map { n -> n * n },
      log { value -> println(value) },
      sum { err, value ->
        t.error(err, "sum() passes with no error")
        t.equal(value, 385)
        t.end()
      })
  }

  test("pull() nested") { t ->
    pull(
      values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
      pull(
        map { n -> n * n },
        log { value -> println(value) }
      ),
      sum { err, value ->
        t.error(err, "sum() passes with no error")
        t.equal(value, 385)
        t.end()
      })
  }

  test("pull() throws when sink called twice") { t ->
    var stream: Sink? = null
    stream = pull(
      map { n -> n * n },
      sum { err, value ->
        t.equal(value, 385, "map reduce successful")
      })

    if (null != stream) {
      stream(values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      t.throws({ stream(values(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)) })
    }

    t.end()
  }
}
