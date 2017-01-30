package com.knoldus.etl

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import java.util.Scanner

import scala.io.Source

/**
  * Created by ANUJ and RAMANDEEP on 1/28/2017.
  */

trait ReadWriteSource {

  def ifExists(string: String):Boolean

  def read(path: String):String

  def write(destination: String,data: String):String

}

object SourceFile extends ReadWriteSource {

  override def ifExists(path: String) = Files.exists(Paths.get(path))

  override def read(path:String) = {
    Source.fromFile(path).mkString
  }

  override def write(destination: String,data: String) = {
    val printWriter = new PrintWriter(new File(destination))
    printWriter.write(data)
    printWriter.close()
    "Data Written to file"
  }

}

trait ReadData {

  val rwSource : ReadWriteSource

  def reading(path: String):String = rwSource.read(path)

  def writing(destination: String,data:String): String = rwSource.write(destination,data)

  def getWordArray(data: String):Array[String] = data.toLowerCase.split("[^a-z0-9']+")

  def getWordsInTotal(data: String):Int = getWordArray(data).length

  def getUniqueWords(path: String):String = getWordMap(reading(path)).toList.map(_._1).mkString(" ")

  def getWordMap(data: String):Map[String,List[String]] = getWordArray(data).toList.groupBy(x=>x)

  def getWordCount(path: String):String = {

    getWordMap(reading(path)).mapValues(x=>x.length) map {
      case (key,value) => key+"->"+value
    } mkString ("\n")

  }

  def writeCaps(destination: String,data: String):String = writing(destination,data.toUpperCase)

}

object ReadWriteFile extends ReadData {

  override val rwSource: ReadWriteSource = SourceFile

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

}

object ETL {

  def main(args: Array[String]): Unit = {

    val scan = new Scanner(System.in)

    println("Enter a directory path to see the files: ")
    val dirPath = scan.next()
    val files = ReadWriteFile.getListOfFiles(dirPath)
    println(s"List of Files: \n$files")

    println("Enter the file name with path to work on: ")
    val sourcePath = scan.next()

    if(ReadWriteFile.rwSource.ifExists(sourcePath)){

      val data = ReadWriteFile.reading(sourcePath)
      println(s"Data Read from file: $sourcePath\n$data\n")

      val dataUpperCase = data.toUpperCase
      println(s"Data Read from file(capitalised): $sourcePath\n$dataUpperCase\n")

      println("Enter file name with path to write to:")
      val destPath = scan.next()
      println(s"writing capitalised data to destination path:$destPath \n ${ReadWriteFile.writeCaps(destPath,data)}\n")

    }
    else{
      println("File Not Found/Error 404")
    }

    println("Enter the path with file name to find unique words")
    val filePath = scan.next()
    if(ReadWriteFile.rwSource.ifExists(filePath)){

      val uniqueWordString = ReadWriteFile.getUniqueWords(sourcePath)
      println(s"Unique words in source file: \n$uniqueWordString\n")

      val summary = ReadWriteFile.getWordCount(sourcePath)
      println(s"Summary for word count:\n$summary\n")

      println("Enter New Path including file name to write summary:")
      val newPath = scan.next()
      println(s"Writing summary to: $newPath  \n${ReadWriteFile.writing(newPath,summary)} \n")

    }
    else{
      println("File Not Found/Error 404")
    }
  }

}