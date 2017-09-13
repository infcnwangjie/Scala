import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props

class Hello extends Actor {
  def receive = {
    case msg: String => println("hello " + msg)
    case _ => println("unexpected message.")
  }
}


object my{
  def test(): Unit ={
    val system = ActorSystem("HelloSystem")
    val hello = system.actorOf(Props[Hello], name = "hello")
    val hello1 = system.actorOf(Props[Hello])
    val hello2 = system.actorOf(Props(new Hello()))
    hello ! "bruce"
    hello ! 10086
    hello1 ! 90
    hello2 ! "batman"
  }
  def main(args: Array[String]): Unit = {
    println("good")
    test()
  }
}
