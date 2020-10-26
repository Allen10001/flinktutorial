package com.tv.scala.learn

object HelloWorldScala {
  def main(args : Array[String]) : Unit = {
    val foo =
      """
        | hello world,
        | hello flink.
      """
    println(foo)

    // 符号字面量
    println('x)

    /**
      *  在 Scala 中声明变量和常量不一定要指明数据类型，在没有指明数据类型的情况下，其数据类型是通过变量或常量的初始值推断出来的。
      *  所以，如果在没有指明数据类型的情况下声明变量或常量必须要给出其初始值，否则将会报错。
      */
    var myVar = "hello"
    val myVal : String = "hello"

    myVar = "world"

    println(myVar)



  }
}

class Super1{

    protected def f(): Unit ={
      println("f")
    }
     class Sub extends Super1{
       f()
     }

    class Other{
      def main(args: Array[String]): Unit = {
        new Super1().f()
      }
    }
  }

class Super2{

    protected def f(): Unit ={
      println("f")
    }
     class Sub extends Super2{
       f()
     }

    class Other{
      def main(args: Array[String]): Unit = {
        new Super2().f()
      }
    }
  }

/**
  * private[x]
  * 或
  * protected[x]
  * 这里的x指代某个所属的包、类或单例对象。如果写成private[x],读作"这个成员除了对[…]中的类或[…]中的包中的类及它们的伴生对像可见外，对其它所有类都是private。
  */
