# Job2 DAG Notes

## Overview
The Job2 DAG is responsible for orchestrating the data processing workflow for the Job2 application. It is scheduled to run daily and ensures that all necessary tasks are completed in a timely manner.

## DAG Structure
- **Task 1:** Data Extraction
    - Extracts data from the source system.
- **Task 2:** Data Transformation
    - Transforms the extracted data into the required format.
- **Task 3:** Data Loading
    - Loads the transformed data into the target system.
- **Task 4:** Data Validation
    - Validates the loaded data to ensure accuracy and completeness.

## Schedule
- **Frequency:** Daily
- **Start Time:** 02:00 AM UTC

## Dependencies
- **Upstream:** None
- **Downstream:** Job3 DAG

## Monitoring
- **Alerts:** Email notifications are sent on failure.
- **Logs:** Logs are stored in `/home/prositadmin/logs/job2/`.

## Contact
- **Owner:** Prosit Admin Team
- **Email:** prositadmin@example.com


-- Edit this.