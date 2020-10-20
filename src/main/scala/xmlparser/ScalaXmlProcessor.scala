package xmlparser

import java.io.{FileReader, FileWriter, Reader}
import java.util
import java.util.Collections

import javax.xml.stream.events.{Attribute, Namespace, XMLEvent}
import javax.xml.stream.{XMLEventFactory, XMLEventReader, XMLEventWriter, XMLInputFactory, XMLOutputFactory, XMLStreamConstants, XMLStreamException}
import javax.xml.namespace.QName

import scala.annotation.tailrec

/**
 * This object will parse the entire dblp.xml file and create smaller xml files from each one,
 * preserving tag consistencies.
 */
object ScalaXmlProcessor {

  def main(args: Array[String]): Unit = {
    System.setProperty("entityExpansionLimit", String.valueOf(Integer.MAX_VALUE))
    val inputFactory: XMLInputFactory = XMLInputFactory.newInstance()
    val reader: Reader = new FileReader("src/main/resources/inputs/dblp.xml")

    val writerFactory: XMLOutputFactory = XMLOutputFactory.newInstance()
    val eventFactory: XMLEventFactory = XMLEventFactory.newInstance()

    val writer: XMLEventWriter = writerFactory.createXMLEventWriter(new FileWriter("src/main/resources/inputs/shards/dblp-copy0.xml"))
    val eventReader: XMLEventReader = inputFactory.createXMLEventReader(reader)

    processTagProps(writer, writerFactory, eventFactory, eventReader, 0, 0, 0,
      0, 0, 0, 0, 1)
  }

  /**
   * This function will process the tags one by one until the end and writes the collected xml tags to the output file
   * once enough tags have been processed for the chunk.
   * @param writer The xml writer object
   * @param writerFactory The xml output factory to create new writer objects for new chunks
   * @param eventFactory The event factory object which will create tags to be written to the chunks for
   *                     each of the following events - Start of document, start of element, end of element,
   *                     character / text data and end of element
   * @param eventReader The reader object which parses the dblp.xml file
   * @param articleCount Number of article entries encountered from the start
   * @param inproceedings number of inproceedings entries encountered from the start
   * @param inCollection number of incollection entries encountered from the start
   * @param book number of book entries encountered from the start
   * @param phdthesis number of phdthesis entries encountered from the start
   * @param mastersthesis number of mastersthesis entries encountered from the start
   * @param totalCount total number of xml tags encountered from the start
   * @param shardIndex index of current chunk being written to
   */
  @tailrec
  def processTagProps(writer: XMLEventWriter, writerFactory: XMLOutputFactory, eventFactory: XMLEventFactory,
                      eventReader: XMLEventReader, articleCount: Int, inproceedings: Int, inCollection: Int, book: Int,
                      phdthesis: Int, mastersthesis: Int, totalCount: Int, shardIndex: Int): Unit = {
    if (eventReader.hasNext) {
      val event: XMLEvent = eventReader.nextEvent()
      writeEventToFile(writer, eventFactory, event)

      if (event.getEventType == XMLStreamConstants.START_ELEMENT) {
        event.asStartElement().getName.toString match {
          case "article" => processTagProps(writer, writerFactory, eventFactory, eventReader, articleCount+1, inproceedings,
            inCollection, book, phdthesis, mastersthesis, totalCount+1, shardIndex)

          case "inproceedings" => processTagProps(writer, writerFactory, eventFactory, eventReader, articleCount, inproceedings+1,
          inCollection, book, phdthesis, mastersthesis, totalCount+1, shardIndex)

          case "incollection" => processTagProps(writer, writerFactory, eventFactory, eventReader, articleCount, inproceedings,
          inCollection+1, book, phdthesis, mastersthesis, totalCount+1, shardIndex)

          case "book" => processTagProps(writer, writerFactory, eventFactory, eventReader, articleCount, inproceedings,
            inCollection, book+1, phdthesis, mastersthesis, totalCount+1, shardIndex)

          case "phdthesis" => processTagProps(writer, writerFactory, eventFactory, eventReader, articleCount, inproceedings,
            inCollection, book, phdthesis+1, mastersthesis, totalCount+1, shardIndex)

          case "mastersthesis" => processTagProps(writer, writerFactory, eventFactory, eventReader, articleCount, inproceedings,
            inCollection, book, phdthesis, mastersthesis+1, totalCount+1, shardIndex)

          case _ => processTagProps(writer, writerFactory, eventFactory, eventReader, articleCount, inproceedings,
            inCollection, book, phdthesis, mastersthesis, totalCount+1, shardIndex)
        }
      }

      else {
        if (event.getEventType == XMLStreamConstants.END_ELEMENT &&
          (totalCount >= shardIndex*500000) &&
          List("article", "inproceedings", "incollection", "book", "phdthesis", "mastersthesis")
            .contains(event.asEndElement().getName.toString)) {
            val newWriter=
              writerFactory.createXMLEventWriter(new FileWriter("src/main/resources/inputs/shards/dblp-copy"+shardIndex+".xml"))
            val iter: util.Iterator[Namespace] = Collections.emptyIterator[Namespace]
            val attrIter: util.Iterator[Attribute] = Collections.emptyIterator[Attribute]

            writeEventToFile(writer, eventFactory, event)
            writeEventToFile(writer, eventFactory, eventFactory.createEndElement(new QName("dblp"), iter))
            writer.flush()
            writer.close()
            writeEventToFile(newWriter, eventFactory, eventFactory.createStartDocument)
            writeEventToFile(newWriter, eventFactory, eventFactory.createStartElement(new QName("dblp"), attrIter, iter))
            processTagProps(newWriter, writerFactory, eventFactory, eventReader, articleCount, inproceedings, inCollection,
              book, phdthesis, mastersthesis, totalCount, shardIndex+1)
        }
        else
          processTagProps(writer, writerFactory, eventFactory, eventReader, articleCount, inproceedings, inCollection,
            book, phdthesis, mastersthesis, totalCount, shardIndex)
      }
    }
    else println(s"Article count - $articleCount\nInproceedings count - $inproceedings" +
      s"\nIncollection count - $inCollection\nBook count - $book\nPhdthesis count - $phdthesis" +
      s"\nMastersthesis count - $mastersthesis\nTotal tag count - $totalCount")
  }

  /**
   * Creates tags for appropriate events and writes them to the writer object
   */
  def writeEventToFile(writer: XMLEventWriter, eventFactory: XMLEventFactory, event: XMLEvent): Unit = {
    try {
      if (event.getEventType == XMLStreamConstants.START_DOCUMENT) {
        writer.add(XMLEventFactory.newInstance().createStartDocument)

      } else if (event.getEventType == XMLStreamConstants.START_ELEMENT)
        writer.add(eventFactory.createStartElement(event.asStartElement().getName, event.asStartElement().getAttributes, event.asStartElement().getNamespaces))

      else if (event.getEventType == XMLStreamConstants.END_ELEMENT)
        writer.add(eventFactory.createEndElement(event.asEndElement().getName, event.asEndElement().getNamespaces))

      else if (event.getEventType == XMLStreamConstants.CHARACTERS)
        writer.add(eventFactory.createCharacters(event.asCharacters().getData))

//      else println("there is something else which is not being considered")

    }
    catch {
      case e: XMLStreamException => e.printStackTrace()
    }
  }
}
