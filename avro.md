# Avro Overview and Examples <!-- omit in toc -->

## Contents <!-- omit in toc -->

- [1. The need for a schema registry](#1-the-need-for-a-schema-registry)
- [2. Kafka Ecosystem: Confluent Schema Registry](#2-kafka-ecosystem-confluent-schema-registry)
- [3. An Evolution of data](#3-an-evolution-of-data)
  - [3.1. Comma Separated Values (CSV)](#31-comma-separated-values-csv)
  - [3.2. Relational tables definitions](#32-relational-tables-definitions)
  - [3.3. JSON (JavaScript Object Notation)](#33-json-javascript-object-notation)
  - [3.4. Avro](#34-avro)
  - [3.5. Avro vs Protobuf vs Thrift vs Parquet vs ORC vs ...](#35-avro-vs-protobuf-vs-thrift-vs-parquet-vs-orc-vs-)
- [4. Avro](#4-avro)
  - [4.1. Primitive Types](#41-primitive-types)
  - [4.2. Record Schemas](#42-record-schemas)
  - [4.3. Complex Types](#43-complex-types)
  - [4.4. Enums](#44-enums)
  - [4.5. Arrays](#45-arrays)
  - [4.6. Maps](#46-maps)
  - [4.7. Unions](#47-unions)
  - [4.8. Logical Types](#48-logical-types)
- [5. Avro Java Schemas](#5-avro-java-schemas)
  - [5.1. Generic Record](#51-generic-record)
  - [5.2. Specific Record](#52-specific-record)
- [6. Schema Evolution Business problem](#6-schema-evolution-business-problem)
  - [6.1. Backward Compatible](#61-backward-compatible)
  - [6.2. Forward Compatible](#62-forward-compatible)
  - [6.3. Fully Compatible](#63-fully-compatible)
  - [6.4. Not Compatible](#64-not-compatible)
  - [6.5. Advice when writing an Avro schema](#65-advice-when-writing-an-avro-schema)
- [7. Schema Registry](#7-schema-registry)

# 1. The need for a schema registry

- Kafka takes bytes as an input and publishes them.
- No data verification.
  - ![Schema Registry](/images/schema-registry.png)
- What if the **producer** sends bad data?
- What if a field gets renamed?
- What if the data format changes from one day to another?
  - **The Consumers Break!!!**
- We need data to be self describable.
- We need to be able to evolve data without breaking downstream consumers.
- We need schemas... and a schema registry!
- What if the Kafka Brokers were verifying the messages they receive?
- **It would break what makes Kafka so good**
  - Kafka doesn't parse or even read your data (no CPU usage).
  - Kafka takes bytes as an input without even loading them into memory (that's called zero copy).
  - Kafka distributes bytes.
  - As far as Kafka is concerned, it doesn't even know if your data is an integer, a string etc.

# 2. Kafka Ecosystem: Confluent Schema Registry

- The Schema Registry has to be a separate components.
- **Producers** and **Consumers** need to be able to talk to it.
- The Schema Registry must be able to reject bad data.
- **A common data format must be agreed upon**
  - It needs to support schemas
  - It needs to support evolution
  - It needs to be lightweight
- Enter... the Confluent Schema Registry.
- And Apache Avro as the data format..
  ![Confluent Schema Registry](/images/confluent-schema-registry.png)

# 3. An Evolution of data

## 3.1. Comma Separated Values (CSV)

| rownum | column1 | column2 | column3 | column4  | column5 | column6 |
| ------ | ------- | ------- | ------- | -------- | ------- | ------- |
| row1   | John    | Doe     | 25      | John.doe | true    | OK      |
| row2   | Mary    | Poppins | sixty   | Mary.pop | yes     | OK      |
| row3   | Tom     | Cruise  | 45      | Tom.Cru  |         |         |

- **Advantages**
  - Easy to parse.
  - Easy to read.
  - Easy to make sense of.
- **Disadvantages**
  - The data types of elements has to be inferred and is not a guarantee.
  - Parsing becomes tricky when data contains commas.
  - Column names may or may not be there.

## 3.2. Relational tables definitions

- Relational table definitions add types:
  ```sql
    CREATE TABLE distributors (
      id INTEGER PRIMARY KEY,
      name VARCHAR(40)
    );
  ```
- **Advantages**
  - Data is fully typed.
  - Data is in a table.
- **Disadvantages**
  - Data has to be flat.
  - Data is stored in a database, and data definition will be different for each database.

## 3.3. JSON (JavaScript Object Notation)

- JSON format can be shared across the network!
  ```json
  {
    "id": "0001",
    "type": "donut",
    "name": "Cake",
    "image": {
      "url": "/images/0001.jpg",
      "width": 200,
      "height": 200
    },
    "thumbnail": {
      "url": "/images/thumbnails/0001.jpg",
      "width": 32,
      "height": 32
    }
  }
  ```
- **Advantages**
  - Data can take any form (arrays, nested elements).
  - JSON is a widely accepted format on the web.
  - JSON can be read by pretty much any language.
  - JSON can be easily shared over a network.
- **Disadvantages**
  - Data has no schema enforcing.
  - JSON Objects can be quite big in size because of repeated keys.

## 3.4. Avro

- Avro is defined by a schema (schema is written in JSON).
- To get started, you can view Avro as JSON with a schema attached to it.
- **Advantages**
  - Data is fully typed.
  - Data is compressed automatically (less CPU usage).
  - Schema (defined using JSON) comes along with the data.
  - Documentation is embedded in the schema.
  - Data can be read across any language.
  - Schema can evolve over time, in a safe manner (schema evolution).
- **Disadvantages**
  - Avro support for some languages may be lacking (but the main ones is fine).
  - Can't "print" the data without a library to unbox (because it's compressed and serialised).

## 3.5. Avro vs Protobuf vs Thrift vs Parquet vs ORC vs ...

- Overall, all of these data formats achieve pretty much the same goal.
- At Kafka's level, what we care about is someone being self explicit and fully described as we're dealing with streaming (so no ORC Parquet etc).
- Avro has good support from Hadoop based technologies like Hive.
- Avro has been chosen as the only supported data format from Confluent Schema Registry so we'll just go along with that!
- There is no need to compare performance etc unless you can prove that Avro is indeed a performance roadblock in your programs (and that won't happen unless you reach insane volumes of 1 million messages per sec).

# 4. Avro

## 4.1. Primitive Types

- **Primitive Types are the support base types**
  - `null` - No value.
  - `boolean` - A binary value.
  - `int` - 32-bit signed integer.
  - `long` - 64-bit signed integer.
  - `float` - Single precision (32-bit) IEEE 754 floating-point number.
  - `double` - Double precision (64-bit) IEEE 754 floating-point number.
  - `bytes` - Sequence of 8-bit unsigned bytes.
  - `string` - Unicode character sequence.

## 4.2. Record Schemas

- Record Schemas are defined using JSON.
- **It has some common fields**
  - `Name` - Name of your schema.
  - `Namespace` - (equivalent of package in Java).
  - `Doc` - Documentation to explain your schema.
  - `Aliases` - Optional other names for your schema.
  - **Fields**
    - `Name` - Name of your field.
    - `Doc` - Documentation for that field.
    - `Type` - Data type for that field (can be a primitive type).
    - `Default` - Default value for that field.

## 4.3. Complex Types

- **In Avro you have complex types, such as**
  - Enums.
  - Arrays.
  - Maps.
  - Unions.
  - Calling other schemas as types.
- Let's go over these types one by one.

## 4.4. Enums

- These are for fields you know for sure that their values can be enumerated.
- **Example:** Customer status.
  - Bronze
  - Silver
  - Gold
- **Example**
  ```json
  {
    "type": "enum",
    "name": "CustomerStatus",
    "symbols": ["BRONZE", "SILVER", "GOLD"]
  }
  ```
- Note: Once an enum is set, changing the enum values is forbidden if you want to maintain compatibility.

## 4.5. Arrays

- Arrays are a way for you to represent a list of undefined size of items that all share the same schema.
- **Example:** Customer Emails (multiple emails).
  - ["john.doe@gmail.com", "jon92@hotmail.com"]
- **Example**
  ```json
  {
    "type": "array",
    "items": "string"
  }
  ```
- **Note:** The schema can be anything you want so you can use any existing schema for it.

## 4.6. Maps

- Maps are a way to define a list of keys and values, where the keys are strings.
- **Example:** Secrets questions.
  - "What's your favourite colour?": "green"
  - "Where were you born?": "Paris"
  - "Name of first pet?": "Mr Snuggles"
- **Example**
  ```json
  {
    "type": "map",
    "values": "string"
  }
  ```
- **Note:** Don't store secrets in Avro. This is just to illustrate the concepts of maps.

## 4.7. Unions

- Unions can allow a field value to take different types.
- **Example:** ["string", "int", "boolean"].
- If defaults are defined, the default must be of the type of the first item in the union (so in this case, "string").
- The most common use case for unions is to define an optional value.
- **Example**
  ```json
  {
    "name": "middle_name",
    "type": ["null", "string"],
    "default": null
  }
  ```
- **Note:** For the default, don't write `"null"`, write `null`.

## 4.8. Logical Types

- Avro has a concept of logical types used to give more meaning to **already existing primitive types**.
- **The most commonly used are**
  - `decimal` (bytes)
  - `date` (int) — number of days since Unix epoch (Jan 1st 1970)
  - `time-millis` (long) — number of milliseconds after midnight, 00:00:00.000
  - `timestamp-millis` (long) — the number of milliseconds from the Unix epoch, Jan 1st 1970 00:00:00.000 UTC
- How to use a logical type?
  - To use a logical type, just add "logicalType": "time-millis" to the field and it will help Avro schema processors to infer a specific type.
- **Example:** Customer Signup Timestamp.
- **Example**
  ```json
  {
    "name": "signup_timestamp",
    "type": "long",
    "logicalType": "timestamp-millis"
  }
  ```
- **Note:** Logical types are new (1.7.7), not fully supported by all languages and don't play nicely with unions.
- Be careful when using them!

# 5. Avro Java Schemas

## 5.1. Generic Record

- A **GenericRecord** is used to create an Avro object from a schema, the schema being referenced as:
  - A file.
  - A strin.g
- It's not the most recommended way of creating Avro objects because things can fail at runtime, but it is the most simple way.

## 5.2. Specific Record

- A **SpecificRecord** is also an Avro object, but it is obtained using code generation from an Avro schema.
- There are different plugins for different build tools (`gradle`, `maven`, `sbt`) etc.
  - Avro Schema → Maven Plugin → Generated Code

# 6. Schema Evolution Business problem

- Avro enables us to evolve our schema over time, to adapt with the changes from the business.
- **For example:** Today we're asking for the **First Name** and **Last Name** of our customer, and that's our v1 of the schema, but tomorrow we ask for their phone number.
  - That would be our v2 of our schema.
- We want to be able to make the schema evolve without breaking programs reading our data.
- **There are 4 kinds of schema evolution**
  - **Backward:** A backward compatible change is when a new schema can be used to read old data.
  - **Forward:** A forward compatible change is when an old schema can be used to read new data.
  - **Full:** Which is both backward and forward.
  - **Breaking:** Which is none of those.

## 6.1. Backward Compatible

- **Backward:** A backward compatible change is when a new schema can be used to read old data.
- We can read old data with the new schema, thanks to a default value. In case the field doesn't exist, Avro will use the default value.
- We want backwards when we want to successfully perform queries (Hive-SQL for example) over old and new data using a new schema.

## 6.2. Forward Compatible

- **Forward:** A forward compatible change is when an old schema can be used to read new data.
- We can read new data with the old schema. Avro will just ignore new fields. Deleting fields without defaults is not forward compatible.
- We want forward compatible when we want to make a data stream evolve without changing our downstream consumers.

## 6.3. Fully Compatible

- **Full:** which is both **Backward** and **Forward**.
- Only add fields with defaults.
- Only remove fields that have defaults.
- When writing your schema changes, most of the time you want to target full compatibility (and it's not too hard, is it?).

## 6.4. Not Compatible

- Here are examples of changes that are NOT compatible:
  - Adding / Removing elements from an Enum.
  - Changing the type of a field (string → int for example).
  - Renaming a required field (without default).

## 6.5. Advice when writing an Avro schema

- Make your primary key required.
- Give default values to all the fields that could be removed in the future.
- Be very careful when using Enums as they can't evolve over time.
- Don't rename fields and call aliases instead (other names).
- When evolving a schema, **ALWAYS** give default values.
- When evolving a schema, **NEVER** delete a required field.

# 7. Schema Registry
