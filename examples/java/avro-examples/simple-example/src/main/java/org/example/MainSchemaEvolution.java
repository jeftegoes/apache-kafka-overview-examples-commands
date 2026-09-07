package org.example;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class MainSchemaEvolution {
    private final static String FILE_NAME_V1 = "customer-generic-v1.avro";
    private final static String FILE_NAME_V2 = "customer-generic-v2.avro";

    static void main() {
        System.setProperty(
                "org.apache.avro.SERIALIZABLE_CLASSES",
                "org.example.Customer, org.example.CustomerV1, org.example.CustomerV2"
        );

        testCustomerV1();
//        testCustomerV2();
    }

    private static void testCustomerV2() {
        CustomerV2 customerv2 = getCustomerV2();
        writeV2ToAFile(customerv2);
        readAvroV2File();
    }

    private static CustomerV2 getCustomerV2() {
        CustomerV2 customerv2 = CustomerV2.newBuilder()
                .setAge(25)
                .setFirstName("Brenno")
                .setLastName("Salvador")
                .setEmail("brenno@gmail.com")
                .setHeight(100f)
                .setWeight(30.2f)
                .setPhoneNumber("1091234-5678")
                .build();

        System.out.println("Customer V2 = " + customerv2.toString());
        return customerv2;
    }

    private static void testCustomerV1() {
        CustomerV1 customerV1 = getCustomerV1();
        writeV1ToAFile(customerV1);
        readAvroV1File();
    }

    private static CustomerV1 getCustomerV1() {
        CustomerV1 customerV1 = CustomerV1.newBuilder()
                .setAge(35)
                .setAutomatedEmail(false)
                .setFirstName("Jefté")
                .setLastName("Goes")
                .setHeight(170f)
                .setWeight(80.5f)
                .build();

        System.out.println("Customer V1 = " + customerV1.toString());
        return customerV1;
    }

    private static void readAvroV1File() {
        System.out.println("Reading our customerV1.avro with v1 schema");
        final File file = new File(FILE_NAME_V1);
        final DatumReader<CustomerV2> datumReader = new SpecificDatumReader<>(CustomerV2.class);
        try (DataFileReader<CustomerV2> dataFileReader = new DataFileReader<>(file, datumReader)) {
            while (dataFileReader.hasNext()) {
                CustomerV2 customerV2 = dataFileReader.next();
                System.out.println(customerV2);
                System.out.println("Customer V2: " + customerV2.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void readAvroV2File() {
        System.out.println("Reading our customerV1.avro with v2 schema");
        final File file = new File(FILE_NAME_V2);
        final DatumReader<CustomerV1> datumReader = new SpecificDatumReader<>(CustomerV1.class);
        try (DataFileReader<CustomerV1> dataFileReader = new DataFileReader<>(file, datumReader)) {
            while (dataFileReader.hasNext()) {
                CustomerV1 customerV2 = dataFileReader.next();
                System.out.println(customerV2);
                System.out.println("Customer V2: " + customerV2.toString());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeV1ToAFile(CustomerV1 customer) {
        final DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<>(CustomerV1.class);
        try (DataFileWriter<CustomerV1> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File(FILE_NAME_V1));
            dataFileWriter.append(customer);
            System.out.println("Written " + FILE_NAME_V1);
        } catch (IOException e) {
            System.out.println("Couldn't write file.");
            e.printStackTrace();
        }
    }

    private static void writeV2ToAFile(CustomerV2 customer) {
        final DatumWriter<CustomerV2> datumWriter = new SpecificDatumWriter<>(CustomerV2.class);
        try (DataFileWriter<CustomerV2> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File(FILE_NAME_V2));
            dataFileWriter.append(customer);
            System.out.println("Written " + FILE_NAME_V2);
        } catch (IOException e) {
            System.out.println("Couldn't write file.");
            e.printStackTrace();
        }
    }
}
