package main

class MainSpec extends munit.FunSuite {
  test("say hello") {
    assertEquals(Main.greeting, "hello")
  }
}
