package com.cgnal

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.AvroType

import scala.util.Success

/**
  * Created by cgnal on 29/09/16.
  */
object ExampleScalAvro{

  case class Example(var version: Long= 3L,
                   var id: Option[String]= Some("hello"),
                   var ts:Long = System.currentTimeMillis(),
                   var event_type_id: Int,
                   var source:String = "",
                   var location:String = "",
                   var host:String = "",
                   var service: String = "",
                   var body: Option[Array[Byte]]= Some("hello world".getBytes()),
                   var attributes: Map[String,String]
                  )

  def main(args: Array[String]): Unit = {
    val schema = AvroType[Example]
    val buf = new ByteArrayOutputStream()
    //val data = Example(1L, System.currentTimeMillis(), "id1")
    val data = Example(
     // version= 2L,
     // id = None,
      event_type_id = 1,
     // source = "source1",
     // location = "42.10,80.765",
     // body = Some("this is an example".getBytes()),
      attributes = Map("tag" -> "value")
    )


    schema.io.write(data, buf)

    println(buf.toByteArray)

    val bis = new ByteArrayInputStream(buf.toByteArray)
    val io: AvroTypeIO[Example] = schema.io
    val Success(ex) = io.read(bis)
    buf.reset()
    println(ex)
  }

}
