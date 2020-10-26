package com.tv.scala.learn

object OperationCharacter {
  def main(args: Array[String]) {
    var a = 10;
    var b = 20;
    println("a == b = " + (a == b) );
    println("a != b = " + (a != b) );
    println("a > b = " + (a > b) );
    println("a < b = " + (a < b) );
    println("b >= a = " + (b >= a) );
    println("b <= a = " + (b <= a) );

    val c = true
    val d = false

    println("c && d = " + (c && d))

    println("c || d = " + (c || d))

    println("!(c && d) = " + !(c && d))
  }
}
