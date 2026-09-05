package org.example;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class MainSpecificRecord {
    private final static String FILE_NAME = "customer-generic.avro";

    static void main() {
        System.setProperty(
                "org.apache.avro.SERIALIZABLE_CLASSES",
                "org.example.Customer"
        );

        Customer.Builder customerBuilder = Customer.newBuilder();
        customerBuilder.setAge(25);
        customerBuilder.setFirstName("Jefté");
        customerBuilder.setLastName("Goes");
        customerBuilder.setAge(35);
        customerBuilder.setHeight(170f);
        customerBuilder.setWeight(80.5f);
        customerBuilder.setAutomatedEmail(false);

        Customer customer = customerBuilder.build();

        System.out.println(customer);

        writeToAFile(customer);

        readAvroFile();
    }

    private static void readAvroFile() {
        final File file = new File(FILE_NAME);
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
        try (DataFileReader<Customer> dataFileReader = new DataFileReader<>(file, datumReader)) {
            while (dataFileReader.hasNext()) {
                Customer readCustomer = dataFileReader.next();
                System.out.println(readCustomer);
                System.out.println("First name: " + readCustomer.getFirstName());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeToAFile(Customer customer) {
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File(FILE_NAME));
            dataFileWriter.append(customer);
            System.out.println("Written " + FILE_NAME);
        } catch (IOException e) {
            System.out.println("Couldn't write file.");
            e.printStackTrace();
        }
    }
}
