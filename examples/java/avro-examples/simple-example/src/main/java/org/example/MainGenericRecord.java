package org.example;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class MainGenericRecord {
    private final static String FILE_NAME = "customer-generic.avro";

    static void main() {
        Schema schema = getSchema();

        GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
        customerBuilder.set("first_name", "Jefté");
        customerBuilder.set("last_name", "Goes");
        customerBuilder.set("age", 35);
        customerBuilder.set("height", 170f);
        customerBuilder.set("weight", 80.5f);
        customerBuilder.set("automated_email", false);
        GenericData.Record customer = customerBuilder.build();
        System.out.println(customer);

        GenericRecordBuilder customerBuilderWithDefault = new GenericRecordBuilder(schema);
        customerBuilderWithDefault.set("first_name", "Brenno");
        customerBuilderWithDefault.set("last_name", "Salvador");
        customerBuilderWithDefault.set("age", 10);
        customerBuilderWithDefault.set("height", 100f);
        customerBuilderWithDefault.set("weight", 30.2f);
        GenericData.Record customerWithDefault = customerBuilderWithDefault.build();
        System.out.println(customerWithDefault);

        writeToAFile(schema, customer);

        readAvroFile();
    }

    private static Schema getSchema() {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse("""
                {
                  "type": "record",
                  "namespace": "com.example",
                  "name": "Customer",
                  "doc": "Avro schema for a Customer",
                  "fields": [
                    {
                      "name": "first_name",
                      "type": "string",
                      "doc": "Customer's first name"
                    },
                    {
                      "name": "last_name",
                      "type": "string",
                      "doc": "Customer's last name"
                    },
                    {
                      "name": "age",
                      "type": "int",
                      "doc": "Customer's age in years"
                    },
                    {
                      "name": "height",
                      "type": "float",
                      "doc": "Customer's height in centimeters"
                    },
                    {
                      "name": "weight",
                      "type": "float",
                      "doc": "Customer's weight in kilograms"
                    },
                    {
                      "name": "automated_email",
                      "type": "boolean",
                      "default": true,
                      "doc": "Indicates whether automated emails are enabled"
                    }
                  ]
                }""");
    }

    private static void readAvroFile() {
        final File file = new File(FILE_NAME);
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        GenericRecord customerRead;

        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)) {
            customerRead = dataFileReader.next();

            System.out.println("Successfully read avro file.");
            System.out.println(customerRead.toString());

            System.out.println("First name: " + customerRead.get("first_name"));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeToAFile(Schema schema, GenericData.Record customer) {
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File(FILE_NAME));
            dataFileWriter.append(customer);
            System.out.println("Written " + FILE_NAME);
        } catch (IOException e) {
            System.out.println("Couldn't write file.");
            e.printStackTrace();
        }
    }
}
