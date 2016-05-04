package src.main.java;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import javax.xml.namespace.QName;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import org.apache.log4j.Logger;

public class Taxonomy {
    private static final Logger log = Logger.getLogger(Taxonomy.class.getName());

    static void usage() {
        System.err.println("");
        System.err.println("usage: java -jar Taxonomy.jar <taxonomy.xml>");
        System.err.println("");
        System.exit(-1);
    }

    public static void main(String[] args) {
        if (args.length == 0 || args.length < 1) {
            usage();
        }

        Connection conn = null;
        try {
            // Establish DB connection.
            conn = DbUtils.getDBConnection();
            if (conn == null) {
                System.exit(-1);
            }

            // Parse taxonomy XML file.
            String fileName = args[0];
            log.info("Parsing XML document (" + fileName + ")...");
            parseXmlFile(conn, fileName);
        }
        catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        finally {
            DbUtils.closeConnection(conn);
        }
    }

    private static void parseXmlFile(Connection conn, String inputFile) {
        try {
        	// First create a new XMLInputFactory.
        	XMLInputFactory inputFactory = XMLInputFactory.newInstance();

            // Set this property to handle special HTML characters like & etc.
            inputFactory.setProperty(XMLInputFactory.IS_COALESCING, true);

        	// Setup a new eventReader.
        	InputStream in = new FileInputStream(inputFile);
        	XMLEventReader eventReader = inputFactory.createXMLEventReader(in);

            DbTaxonomy dbTax = null;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            boolean newCode = false;
            boolean codeName = false;
            boolean definition = false;
            boolean created = false;
            boolean lastModified = false;

        	// Read the XML document.
            //int count = 0;
        	while (eventReader.hasNext()) {
                // @debug.
                //if (count >= 3) {
                //    break;
                //}
        		XMLEvent event = eventReader.nextEvent();
                String qName = "";
                switch (event.getEventType()) {
                    case XMLStreamConstants.START_ELEMENT:
                        StartElement startEle = event.asStartElement();
                        qName = startEle.getName().getLocalPart();
                        switch (qName) {
                            case "record":
                                Attribute codeAttr = startEle.getAttributeByName(new QName("code"));
                                if (codeAttr != null) {
                                    String code = codeAttr.getValue();
                                    dbTax = new DbTaxonomy();
                                    dbTax.code = code;
                                    log.info("Code: " + dbTax.code);
                                    newCode = true;
                                }
                                break;
                            case "name":
                                if (newCode) {
                                    codeName = true;
                                }
                                break;
                            case "definition":
                                if (newCode) {
                                    definition = true;
                                }
                                break;
                            case "createdDate":
                                if (newCode) {
                                    created = true;
                                }
                                break;
                            case "lastModifiedDate":
                                if (newCode) {
                                    lastModified = true;
                                }
                                break;
                            default:
                                break;
                        }
                        break;
                    case XMLStreamConstants.CHARACTERS:
                        Characters chars = event.asCharacters();
                        if (codeName) {
                            String taxonomyName = chars.getData();
                            if (dbTax != null) {
                                dbTax.name = taxonomyName;
                                log.info("Name: " + dbTax.name);
                            }
                            codeName = false;
                        }
                        else if (definition) {
                            String taxonomyDef = chars.getData();
                            if (dbTax != null) {
                                dbTax.definition = taxonomyDef;
                                log.info("Definition: " + dbTax.definition);
                            }
                            definition = false;
                        }
                        else if (created) {
                            String s = chars.getData();
                            if (s != null) {
                                log.info("Date Created: " + s);
                                try {
                                    if (dbTax != null) {
                                        dbTax.created = sdf.parse(s.trim());
                                    }
                                }
                                catch (ParseException e) {
                                    log.error("Error parsing created date!");
                                    e.printStackTrace();
                                }
                            }
                            created = false;
                        }
                        else if (lastModified) {
                            String s = chars.getData();
                            if (s != null) {
                                log.info("Date Last Modified: " + s);
                                try {
                                    if (dbTax != null) {
                                        dbTax.lastModified = sdf.parse(s.trim());
                                    }
                                }
                                catch (ParseException e) {
                                    log.error("Error parsing last modified date!");
                                    e.printStackTrace();
                                }
                            }
                            lastModified = false;
                        }
                        break;
                    case XMLStreamConstants.END_ELEMENT:
                        EndElement endEle = event.asEndElement();
                        qName = endEle.getName().getLocalPart();
                        switch (qName) {
                            case "lastModifiedDate":
                                // Upsert taxonomy info.
                                if (dbTax != null) {
                                    DbTaxonomy old = DbTaxonomy.findByCode(conn, dbTax.code);
                                    if (old != null) {
                                        dbTax.id = old.id;
                                        dbTax.update(conn);
                                    }
                                    else {
                                        dbTax.insert(conn);
                                    }
                                    dbTax = null;
                                }
                                newCode = false;
                                //count++;
                                break;
                            default:
                                break;
                        }
                        break;
                    default:
                        break;
                }
        	}
        }
        catch (FileNotFoundException e) {
            log.error("XML file document not found!\n");
        }
        catch (XMLStreamException e) {
            log.error("Error parsing XML document:\n");
        	e.printStackTrace();
        }
    }
}
