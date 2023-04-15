## Data Pipelines Using Apache AirFlow
In this module, we will be creating a data pipeline using Apache AirFlow. The pipeline will extract data from a web server log file, transform the data, and load the transformed data into a tar file.

## Components
The following components are included in this project:

1. Authoring an Apache Airflow DAG: We will perform a series of tasks to create a DAG that runs daily.

2. Extracting the IP address field: We will create a task that extracts the IP address field from the webserver log file and then saves it into a text file.

3. Filtering out specific IP address: We will create a task that filters out all the occurrences of the IP address "198.46.149.143" from the text file and save the output to a new text file.

4. Loading the data by archiving the transformed text file into a TAR file: In the final task creation, we will load the data by archiving the transformed text file into a TAR file.

!(Screenshot from 2023-04-15 22-30-22.png)
