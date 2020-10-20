package xmlparser

import java.io.{File, FileInputStream, FileWriter, InputStreamReader}
import scala.xml.{Elem, XML}

object XmlParser {
  def main(args: Array[String]): Unit = {
    System.setProperty("entityExpansionLimit", String.valueOf(Integer.MAX_VALUE))
    for (i <- 0 to 137) {
      val xml: Elem = XML.load(new InputStreamReader(new FileInputStream(s"src/main/resources/inputs/shards/dblp-copy$i.xml"), "UTF-8"))
      processTags(xml, i)
    }
  }

  /**
   * This function will process the tags based on the publication type.
   * For e.g. <article mdate="2017-06-08" publtype="informal" key="tr/trier/MI96-05">
   *            <author>Helmut Seidl</author>
   *            <title>Fast and Simple Nested Fixpoints</title>
   *            <journal>Universitt Trier, Mathematik/Informatik, Forschungsbericht</journal>
   *            <volume>96-05</volume>
   *            <year>1996</year>
   *          </article>
   *          For this publication the journal entry is considered as the venue, title as the publication name, year as year
   *          and each author entry as one author for this publication
   * After parsing, the function will write the components to the mapper input file
   */
  def processTags(xml: scala.xml.Elem, shardIndex: Int): Unit = {
    val writer: FileWriter = new FileWriter(new File("src/main/resources/inputs/shards/input"+shardIndex+".txt"))
    (xml \ "article").foreach(article => {
      writer.write(s"`${(article \ "title").text}` ")
      writer.write(s"`${(article \ "year").text}` ")
      writer.write(s"`${(article \ "journal").text}`\t")
      (article \ "author").foreach(author => writer.write(s"`${author.text}` "))
      writer.write("\n")
    })

    (xml \ "inproceedings").foreach(publication => {
      writer.write(s"`${(publication \ "title").text}` ")
      writer.write(s"`${(publication \ "year").text}` ")
      writer.write(s"`${(publication \ "booktitle").text}`\t")
      (publication \ "author").foreach(author => writer.write(s"`${author.text}` "))
      writer.write("\n")
    })

    (xml \ "incollection").foreach(publication => {
      writer.write(s"`${(publication \ "title").text}` ")
      writer.write(s"`${(publication \ "year").text}` ")
      writer.write(s"`${(publication \ "booktitle").text}`\t")
      (publication \ "author").foreach(author => writer.write(s"`${author.text}` "))
      writer.write("\n")
    })

    (xml \ "phdthesis").foreach(publication => {
      writer.write(s"`${(publication \ "title").text}` ")
      writer.write(s"`${(publication \ "year").text}` ")
      writer.write(s"`${(publication \ "publisher").text}`\t")
      (publication \ "author").foreach(author => writer.write(s"`${author.text}` "))
      writer.write("\n")
    })
    writer.flush()
    writer.close()
    println(s"done with shard $shardIndex")
  }
}
