# README for Pub/Sub-Airflow BigQuery Integration Project

## Project Overview

This project demonstrates how to integrate Google Cloud Pub/Sub with Airflow to trigger BigQuery stored procedures based on incoming messages. It consists of a Pub/Sub publisher and an Airflow DAG that processes messages and conditionally triggers one of two stored procedures. The project directories include authentication keys and source code files.

---

## Project Structure

The project is structured as follows:
```
.
├── auth/
│   └── sample_key.json
└── src/
    ├── PubSub Publisher.ipynb
    └── pubsub_airflow_bq_integration.py
```

### Directory Descriptions

- **auth/**: Contains the authentication key file required to authenticate with Google Cloud services.
- **src/**: Contains source code for the Pub/Sub publisher and Airflow integration.

---

## Prerequisites

1. **Google Cloud Account and Project**: Ensure you have a Google Cloud project set up.
2. **Service Account Key**: A valid service account key file for authentication.
3. **Pub/Sub Topic and Subscription**: Configure Pub/Sub with a topic and subscription.
4. **Airflow Setup**: Airflow must be properly installed and configured with access to your Google Cloud project.

---

## Files

### 1. **auth/coastal-throne-433510-a5-9d8c4d581c73.json**

This file is the service account key file for authenticating with Google Cloud services. **Make sure to keep this file secure.** It is used for both publishing messages and connecting Airflow to Google Cloud services.

---

### 2. **src/PubSub Publisher.ipynb**

This Jupyter Notebook publishes messages to a Pub/Sub topic to trigger BigQuery stored procedures via Airflow.

#### Key Components:
- **Google Cloud Authentication**: Set up using the service account key.
- **Pub/Sub Publisher**: Publishes JSON messages with the stored procedure name to the configured Pub/Sub topic.
  
#### Usage:
1. Update `GOOGLE_APPLICATION_CREDENTIALS` with the correct path to the service account key file.
2. Specify the GCP project ID and topic name.
3. Modify the `message` dictionary to set the desired stored procedure name (`sp_table_snapshots` or `sp_dynamic_pivot`).
4. Run the notebook to publish the message.

---

### 3. **src/pubsub_airflow_bq_integration.py**

This Python script defines an Airflow DAG that listens for messages from a Pub/Sub subscription and triggers one of two BigQuery stored procedures based on the content of the message.

#### Key Components:
- **DAG Configuration**: Includes the project ID, subscription name, and BigQuery stored procedures.
- **PubSubPullSensor**: Listens for incoming Pub/Sub messages.
- **PythonOperator for Processing Messages**: Decodes the message and determines which stored procedure to run.
- **Branching Logic**: Decides which stored procedure to trigger.
- **BigQueryInsertJobOperator**: Executes the appropriate stored procedure using BigQuery.

#### DAG Tasks:
1. `wait_for_pubsub_message`: Waits for a Pub/Sub message.
2. `determine_sp_task`: Decodes and processes the message.
3. `branching_task`: Branches execution to the appropriate stored procedure based on the message.
4. `run_sp_table_snapshots`: Runs the `sp_table_snapshots` stored procedure.
5. `run_sp_dynamic_pivot`: Runs the `sp_dynamic_pivot` stored procedure.

---

## How to Use

### 1. Publish a Message to Pub/Sub

- Navigate to `src/PubSub Publisher.ipynb`.
- Update the `message` dictionary with the desired stored procedure name (`sp_table_snapshots` or `sp_dynamic_pivot`).
- Run the notebook to publish the message.

### 2. Airflow DAG Execution

- Ensure Airflow is running.
- The `pubsub_triggered_sp_dag` DAG will automatically pick up the Pub/Sub message.
- The DAG will determine the appropriate stored procedure to trigger based on the message content and execute it using BigQuery.

---

## Security Considerations

- **Service Account Key File**: Keep the `coastal-throne-433510-a5-9d8c4d581c73.json` file secure.
- **Permissions**: Ensure appropriate permissions are set on Google Cloud for Pub/Sub and BigQuery access.

---

## Troubleshooting

- **Message Not Being Processed**: Ensure the correct topic and subscription names are configured.
- **Authentication Errors**: Verify the path and permissions for the service account key file.
- **Airflow Issues**: Check Airflow logs for detailed error messages.

---

This README covers all essential details for setting up and using the Pub/Sub-Airflow BigQuery integration. Modify the configurations as needed for your specific use case. Happy coding!
