
# Breast cancer Detector

This project is written for breast cancer detection using machine learning algorithms in Apache Spark.

The project consists of two parts:

- Offline model
- Online prediction

## Run Locally

Clone the project

```bash
  git clone https://github.com/mjinor/Breast-Cancer.git
```

Go to the project directory

```bash
  cd Breast-Cancer
```

Compile project

```bash
  sbt package
```

run offline section of project to make model

```bash
  spark-submit --class Offline project-directory-path/target/compiled-jar-file.jar
```

