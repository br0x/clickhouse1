# ClickHouse JDBC Example

This project demonstrates how to use JDBC to connect to and interact with a ClickHouse database using Java.

## Prerequisites

- Java 21 or higher
- Maven
- ClickHouse server running on localhost:8123

## Building the project

To build the project, run:

```
mvn clean package
```

## Running the application

To run the application, use:

```
java -jar target/clickhouse-jdbc-1.0-SNAPSHOT.jar
```

## Project Structure

- `src/main/java/com/example/App.java`: Main application class
- `src/main/resources/log4j2.xml`: Logging configuration
- `src/test/java/com/example/AppTest.java`: Test class
- `pom.xml`: Maven project configuration

## License

This project is open-source and available under the MIT License.
