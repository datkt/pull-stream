import datkt.pullstream.test.*

fun main(argv: Array<String>) {
  datkt.pullstream.test.tests().pull(argv)

  datkt.tape.collect()
}
