package com.cgnal

import java.io.{ByteArrayOutputStream, File}
import collection.JavaConverters._
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord, GenericRecordBuilder}
import org.apache.avro.io.EncoderFactory

/**
  * Created by cgnal on 29/09/16.
  */
object ExampleAvro {
  val schema = SchemaBuilder
    .record("event")
    .fields
    .name("version").`type`().longType().longDefault(3L)
    .name("ID").`type`().stringType().noDefault()
    .name("timestamp").`type`().longType().noDefault()
    .endRecord

  def run() = {
    // Build an object conforming to the schema
    val event1 = new GenericRecordBuilder(schema)
      .set("version", 1L)
      .set("ID", "id1")
      .set("timestamp", System.currentTimeMillis())
      .build

    // JSON encoding of the object (a single record)
    val writer = new GenericDatumWriter[GenericRecord](schema)
    val baos = new ByteArrayOutputStream
    val jsonEncoder = EncoderFactory.get.jsonEncoder(schema, baos)
    writer.write(event1, jsonEncoder)
    jsonEncoder.flush
    println("JSON encoded record: " + baos)

    // binary encoding of the object (a single record)
    baos.reset
    val binaryEncoder = EncoderFactory.get.binaryEncoder(baos, null)
    writer.write(event1, binaryEncoder)
    binaryEncoder.flush
    println("Binary encoded record: " + baos.toByteArray)

    // Build another object conforming to the schema
    val event2 = new GenericRecordBuilder(schema)
      .set("version", 1L)
      .set("ID", "id2")
      .set("timestamp", System.currentTimeMillis() + 10)
      .build

    // Write both records to an Avro object container file
    val file = new File("users.avro")
    file.deleteOnExit
    val dataFileWriter = new DataFileWriter[GenericRecord](writer)
    dataFileWriter.create(schema, file)
    dataFileWriter.append(event1)
    dataFileWriter.append(event2)
    dataFileWriter.close

    // Read the records back from the file
    val datumReader = new GenericDatumReader[GenericRecord](schema)
    val dataFileReader = new DataFileReader[GenericRecord](file, datumReader)
    val events = dataFileReader.iterator().asScala.toList

    events.foreach(e=>println("Read user from Avro file: " +e))

  }


  def main(args: Array[String]): Unit = {
    run()
  }
}
