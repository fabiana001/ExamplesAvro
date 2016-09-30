package com.cgnal

import java.nio.ByteBuffer

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase

import scala.annotation.switch
import scala.util.Try

/**
  * Created by cgnal on 29/09/16.
  */
object ExampleBijectionAvro{
  implicit private val specificAvroBinaryInjection: Injection[Example, Array[Byte]] = SpecificAvroCodecs.toBinary[Example]

  def main(args: Array[String]): Unit = {

    val ex1 = Example(
          id = None,
          event_type_id = 1,
          source = "source1",
          location = "42.10,80.765",
          body = Some(ByteBuffer.wrap("this is an example".getBytes())),
          attributes = Map("tag" -> "value"))

   // val ex1 = new Example()

    // This tells Bijection how to automagically deserialize a Java type `T`, given a byte array `byte[]`.
    val pippo: Array[Byte] = specificAvroBinaryInjection(ex1)
    val obj: Try[Example] = specificAvroBinaryInjection.invert(pippo)

    println(pippo)
    println(obj)
  }
}

case class Example(var version: Long = 3L,
                   var id: Option[String]= Some("hello"),
                   var ts:Long = System.currentTimeMillis(),
                   var event_type_id: Int,
                   var source:String = "",
                   var location:String = "",
                   var host:String = "",
                   var service: String = "",
                   var body: Option[ByteBuffer]= Some(ByteBuffer.wrap("hello world".getBytes())),
                   var attributes: Map[String,String]
                  ) extends SpecificRecordBase {

 override def getSchema: Schema = new org.apache.avro.Schema.Parser().parse(getClass.getResourceAsStream("/Event2.avsc"))

  def this() = this(event_type_id=1, attributes=Map("nothing" -> "nothing"))

  override def get(field$: Int): AnyRef = {
    (field$: @switch) match {
      case 0 => version.asInstanceOf[AnyRef]
      case 1 =>
        if(id.isEmpty) null.asInstanceOf[AnyRef] else id.get.asInstanceOf[AnyRef]
      case 2 => ts.asInstanceOf[AnyRef]
      case 3 => event_type_id.asInstanceOf[AnyRef]
      case 4 => source.asInstanceOf[AnyRef]
      case 5 => location.asInstanceOf[AnyRef]
      case 6 => host.asInstanceOf[AnyRef]
      case 7 => service.asInstanceOf[AnyRef]
      case 8 => if(body.isEmpty) null.asInstanceOf[AnyRef] else body.get.asInstanceOf[AnyRef]
      case 9 => attributes.asInstanceOf[AnyRef]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
  }

  override def put(field$: Int, value: Any): Unit = {
    (field$: @switch) match {
      case 0 => this.version.asInstanceOf[Long]
      case 1 => this.id = id.asInstanceOf[Option[String]]
      case 2 => this.ts = value.asInstanceOf[Long]
      case 3 => this.event_type_id = value.asInstanceOf[Int]
      case 4 => this.source = value.asInstanceOf[String]
      case 5 => this.location = value.asInstanceOf[String]
      case 6 => this.host = value.asInstanceOf[String]
      case 7 => this.service = value.asInstanceOf[String]
      case 8 => this.body = value.asInstanceOf[Option[ByteBuffer]]
      case 9 => this.attributes = value.asInstanceOf[Map[String,String]]
      case _ => new org.apache.avro.AvroRuntimeException("Bad index")
    }
    ()
  }
}



