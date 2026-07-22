# AWS Pipeline Documentation

This document describes the AWS infrastructure and deployment architecture for the Highstreets data processing pipelines.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Docker Containerization](#docker-containerization)
- [AWS Batch Configuration](#aws-batch-configuration)
- [Step Functions Orchestration](#step-functions-orchestration)
- [ECR Repository Management](#ecr-repository-management)
- [Pipeline Modules](#pipeline-modules)
- [Environment Variables](#environment-variables)
- [Deployment Process](#deployment-process)
- [Monitoring and Logging](#monitoring-and-logging)

## Overview

The Highstreets package uses AWS cloud infrastructure for automated data processing pipelines. The system is designed around:

- **Docker**: Containerized pipeline modules for consistent execution
- **AWS Batch**: Managed compute environment for running data processing jobs
- **Step Functions**: Workflow orchestration and job dependency management
- **ECR**: Container image repository and versioning
- **RDS**: PostgreSQL database for data storage
- **S3**: Data storage and archiving
- **Lambda**: Date processing and parameter transformation

## Architecture

### High-Level AWS Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Step Functions │───▶│   AWS Batch     │───▶│   RDS/S3        │
│  (Orchestration)│    │   (Compute)     │    │   (Storage)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Lambda         │    │  Docker Images  │    │  Data Tables    │
│  (Date Proc.)   │    │  (ECR)          │    │  (PostgreSQL)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Data Flow

```
Schedule → Step Functions → Lambda (Date Processing) → Parallel Batch Jobs → Database/S3
```

## Docker Containerization

### Container Structure

Each pipeline module is containerized using the following deployment pattern:

```bash
# ECR Login
aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin 590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-msoa

# Build Docker Image
docker build -t msoa-e2e-pipeline .

# Tag for ECR
docker tag msoa-e2e-pipeline:latest 590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-msoa:latest

# Push to ECR
docker push 590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-msoa:latest
```

### Container Images

The following pipeline modules are containerized in ECR:

- **hsds-msoa**: MSOA data processing (`msoa_e2e.py`)
- **hsds-lsoa**: LSOA data processing (`lsoa_e2e.py`)
- **hsds-bt-hex**: Hex grid processing (`hex_e2e.py`)
- **hsds-bt-outage**: BT outage processing (`bt_outage.py`)
- **hsds-daily-totals**: Daily aggregation (`daily_agg.py`)
- **hsds-lookup**: Lookup table processing (`bt_lookups.py`)
- **hsds-mcard-weekly**: Mastercard weekly processing (`mcard_weekly.py`)
- **hsds-mcard-intl**: Mastercard international processing (`mcard_weekly_intl.py`)
- **hsds-mcard-3hourly**: Mastercard 3-hourly processing (`mcard_3hourly.py`)
- **hsds-catchment**: BT catchment visitor/worker processing (`catchment_e2e.py`)

## AWS Batch Configuration

### Job Definitions

The system uses multiple Batch job definitions for different processing tasks:

#### Core Job Definitions
- `hsds-bt-outage-job:1` - BT outage data processing
- `hsds-msoa-job-definition:5` - MSOA data processing
- `hsds-lsoa-job:3` - LSOA data processing
- `hsds-daily-totals-job:2` - Daily aggregation processing
- `hsds-bt-hex-job:1` - Hex grid data processing
- `hsds-lookup-job:1` - Lookup table processing
- `hsds-mcard-weekly:1` - Mastercard weekly processing
- `hsds-mcard-intl:1` - Mastercard international processing
- `hsds-mcard-3hourly:2` - Mastercard 3-hourly processing

- `hsds-catchment-job:1` - BT catchment visitor/worker monthly processing

#### Job Definition Structure
```json
{
  "jobDefinitionName": "hsds-msoa-job-definition",
  "type": "container",
  "revision": 5,
  "containerProperties": {
    "image": "590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-msoa:latest",
    "vcpus": 2,
    "memory": 4096,
    "jobRoleArn": "arn:aws:iam::590183914513:role/HSDSBatchJobRole"
  }
}
```

### Compute Environment

#### Configuration
- **Account ID**: 590183914513
- **Region**: eu-west-2
- **Job Queue**: `hsds-e2e`
- **Queue ARN**: `arn:aws:batch:eu-west-2:590183914513:job-queue/hsds-e2e`
- **Type**: Managed compute environment
- **Instance Types**: Optimal scaling

## Step Functions Orchestration

### BT Data Processing Pipeline

The BT pipeline uses complex parallel execution for efficient data processing:

```json
{
  "QueryLanguage": "JSONata",
  "StartAt": "Parallel",
  "States": {
    "Parallel": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "Process Dates (1)",
          "States": {
            "Process Dates (1)": {
              "Type": "Task",
              "Resource": "arn:aws:states:::lambda:invoke",
              "Arguments": {
                "FunctionName": "arn:aws:lambda:eu-west-2:590183914513:function:HSDSProcessDateParameters:$LATEST",
                "Payload": {
                  "startDate": "{% $states.input.startDate %}",
                  "endDate": "{% $states.input.endDate %}"
                }
              },
              "Next": "Parallel (2)"
            },
            "Parallel (2)": {
              "Type": "Parallel",
              "Branches": [
                {
                  "StartAt": "Parallel (1)",
                  "States": {
                    "Parallel (1)": {
                      "Type": "Parallel",
                      "Branches": [
                        {
                          "StartAt": "Outage Batch Job",
                          "States": {
                            "Outage Batch Job": {
                              "Type": "Task",
                              "Resource": "arn:aws:states:::batch:submitJob.sync",
                              "Arguments": {
                                "JobDefinition": "arn:aws:batch:eu-west-2:590183914513:job-definition/hsds-bt-outage-job:1",
                                "JobQueue": "arn:aws:batch:eu-west-2:590183914513:job-queue/hsds-e2e"
                              }
                            }
                          }
                        },
                        {
                          "StartAt": "MSOA Batch Job",
                          "States": {
                            "MSOA Batch Job": {
                              "Type": "Task",
                              "Resource": "arn:aws:states:::batch:submitJob.sync",
                              "Arguments": {
                                "JobDefinition": "arn:aws:batch:eu-west-2:590183914513:job-definition/hsds-msoa-job-definition:5",
                                "JobQueue": "arn:aws:batch:eu-west-2:590183914513:job-queue/hsds-e2e"
                              }
                            }
                          }
                        },
                        {
                          "StartAt": "LSOA Batch Job",
                          "States": {
                            "LSOA Batch Job": {
                              "Type": "Task",
                              "Resource": "arn:aws:states:::batch:submitJob.sync",
                              "Arguments": {
                                "JobDefinition": "arn:aws:batch:eu-west-2:590183914513:job-definition/hsds-lsoa-job:3",
                                "JobQueue": "arn:aws:batch:eu-west-2:590183914513:job-queue/hsds-e2e"
                              }
                            }
                          }
                        }
                      ]
                    }
                  }
                },
                {
                  "StartAt": "Daily Totals",
                  "States": {
                    "Daily Totals": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::batch:submitJob.sync",
                      "Arguments": {
                        "JobDefinition": "arn:aws:batch:eu-west-2:590183914513:job-definition/hsds-daily-totals-job:2",
                        "JobQueue": "arn:aws:batch:eu-west-2:590183914513:job-queue/hsds-e2e"
                      }
                    }
                  }
                }
              ]
            }
          }
        },
        {
          "StartAt": "HEX Batch Job",
          "States": {
            "HEX Batch Job": {
              "Type": "Task",
              "Resource": "arn:aws:states:::batch:submitJob.sync",
              "Arguments": {
                "JobDefinition": "arn:aws:batch:eu-west-2:590183914513:job-definition/hsds-bt-hex-job:1",
                "JobQueue": "arn:aws:batch:eu-west-2:590183914513:job-queue/hsds-e2e"
              }
            }
          }
        },
        {
          "StartAt": "Lookup job",
          "States": {
            "Lookup job": {
              "Type": "Task",
              "Resource": "arn:aws:states:::batch:submitJob.sync",
              "Arguments": {
                "JobDefinition": "arn:aws:batch:eu-west-2:590183914513:job-definition/hsds-lookup-job:1",
                "JobQueue": "arn:aws:batch:eu-west-2:590183914513:job-queue/hsds-e2e"
              }
            }
          }
        }
      ]
    }
  }
}
```

### Mastercard Processing Pipeline

The Mastercard pipeline uses sequential execution for data dependencies:

```json
{
  "QueryLanguage": "JSONata",
  "StartAt": "Lookup job",
  "States": {
    "Lookup job": {
      "Type": "Task",
      "Resource": "arn:aws:states:::batch:submitJob.sync",
      "Arguments": {
        "JobDefinition": "arn:aws:batch:eu-west-2:590183914513:job-definition/hsds-lookup-job:1",
        "JobQueue": "arn:aws:batch:eu-west-2:590183914513:job-queue/hsds-e2e"
      },
      "Next": "Weekly Batch Job"
    },
    "Weekly Batch Job": {
      "Type": "Task",
      "Resource": "arn:aws:states:::batch:submitJob.sync",
      "Arguments": {
        "JobDefinition": "arn:aws:batch:eu-west-2:590183914513:job-definition/hsds-mcard-weekly:1",
        "JobQueue": "arn:aws:batch:eu-west-2:590183914513:job-queue/hsds-e2e"
      },
      "Next": "Weekly Intl Batch Job"
    },
    "Weekly Intl Batch Job": {
      "Type": "Task",
      "Resource": "arn:aws:states:::batch:submitJob.sync",
      "Arguments": {
        "JobDefinition": "arn:aws:batch:eu-west-2:590183914513:job-definition/hsds-mcard-intl:1",
        "JobQueue": "arn:aws:batch:eu-west-2:590183914513:job-queue/hsds-e2e"
      },
      "Next": "Threehourly Batch Job"
    },
    "Threehourly Batch Job": {
      "Type": "Task",
      "Resource": "arn:aws:states:::batch:submitJob.sync",
      "Arguments": {
        "JobDefinition": "arn:aws:batch:eu-west-2:590183914513:job-definition/hsds-mcard-3hourly:2",
        "JobQueue": "arn:aws:batch:eu-west-2:590183914513:job-queue/hsds-e2e"
      },
      "End": true
    }
  }
}
```

### Workflow Execution Patterns

#### BT Pipeline Features
- **Parallel Execution**: Multiple Batch jobs run simultaneously for efficiency
- **Lambda Integration**: `HSDSProcessDateParameters` function processes date parameters
- **Date Adjustment**: Daily totals job uses adjusted dates (previous day)
- **Month Derivation**: Catchment job derives previous month's 1st from weekly startDate
- **JSONata Query Language**: Advanced JSON processing and transformation

#### Catchment Branch (Branch 4) - Monthly Data
The catchment branch runs in parallel with existing BT jobs. It uses a Pass state
to derive the previous month's first day from the weekly `startDate` input:

```
JSONata: $substring($fromMillis($toMillis($substring($states.input.startDate, 0, 7) & '-01') - 86400000), 0, 7) & '-01'

Example: startDate = "2026-06-15" → catchmentMonth = "2026-05-01"

Steps:
  1. $substring("2026-06-15", 0, 7)         → "2026-06"
  2. & '-01'                                 → "2026-06-01"
  3. $toMillis(...)                           → epoch ms for June 1st
  4. - 86400000                              → May 31st epoch ms
  5. $fromMillis(...)                         → "2026-05-31T..."
  6. $substring(..., 0, 7) & '-01'           → "2026-05-01"
```

The batch job receives `START_DATE=END_DATE=2026-05-01` and performs a pre-flight
S3 existence check. If all Parquet partitions already exist for that month, it exits
immediately (no API call). This makes weekly re-runs cost-free after the first
successful extraction.

Full Step Function JSON with the catchment branch: `docs/step_function_catchment_branch.json`

#### Mastercard Pipeline Features
- **Sequential Processing**: Jobs run in dependency order
- **Data Dependencies**: Each job depends on the previous job's completion
- **Lookup Processing**: Starts with lookup table updates
- **Progressive Processing**: Weekly → International → 3-hourly data

## ECR Repository Management

### Repository Structure

```
590183914513.dkr.ecr.eu-west-2.amazonaws.com/
├── hsds-msoa:latest
├── hsds-lsoa:latest
├── hsds-bt-hex:latest
├── hsds-bt-outage:latest
├── hsds-daily-totals:latest
├── hsds-lookup:latest
├── hsds-mcard-weekly:latest
├── hsds-mcard-intl:latest
├── hsds-mcard-3hourly:latest
└── hsds-catchment:latest
```

### Image Building and Deployment

#### Standard Deployment Process
```bash
# 1. ECR Authentication
aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin 590183914513.dkr.ecr.eu-west-2.amazonaws.com/[repository-name]

# 2. Build Docker Image
docker build -t [service-name]-e2e-pipeline .

# 3. Tag for ECR
docker tag [service-name]-e2e-pipeline:latest 590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-[service-name]:latest

# 4. Push to ECR
docker push 590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-[service-name]:latest
```

#### Example: MSOA Pipeline Deployment
```bash
aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin 590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-msoa
docker build -t msoa-e2e-pipeline .
docker tag msoa-e2e-pipeline:latest 590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-msoa:latest
docker push 590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-msoa:latest
```

#### Example: Catchment Pipeline Deployment
```bash
# 1. Create ECR repository (one-time)
aws ecr create-repository --repository-name hsds-catchment --region eu-west-2

# 2. ECR Authentication
aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin 590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-catchment

# 3. Build, tag, push
docker build -t catchment-e2e-pipeline .
docker tag catchment-e2e-pipeline:latest 590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-catchment:latest
docker push 590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-catchment:latest

# 4. Register job definition (one-time)
aws batch register-job-definition \
  --job-definition-name hsds-catchment-job \
  --type container \
  --container-properties '{
    "image": "590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-catchment:latest",
    "vcpus": 4,
    "memory": 16384,
    "jobRoleArn": "arn:aws:iam::590183914513:role/HSDSBatchJobRole",
    "command": ["poetry", "run", "python", "highstreets/aws_pipeline/catchment_e2e.py"]
  }' \
  --region eu-west-2
```

## Pipeline Modules

### BT Data Processing Modules

#### hex_e2e.py - Hex Grid Processing
```python
# Job Definition: hsds-bt-hex-job:1
# Environment Variables:
# - START_DATE: Processing start date (YYYY-MM-DD)
# - END_DATE: Processing end date (YYYY-MM-DD)
# - PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD: Database connection
# - CONSUMER_KEY, CONSUMER_SECRET: BT API credentials
# - BASE_DIR: S3 base directory

# Process: Fetches hex data from BT API → Transforms → Saves to bt_footfall_tfl_hex_3hourly
```

#### msoa_e2e.py - MSOA Processing
```python
# Job Definition: hsds-msoa-job-definition:5
# Processes MSOA level aggregations from hex data
```

#### lsoa_e2e.py - LSOA Processing
```python
# Job Definition: hsds-lsoa-job:3
# Processes LSOA level aggregations from hex data
```

#### bt_outage.py - Outage Processing
```python
# Job Definition: hsds-bt-outage-job:1
# Processes BT API outage data and updates tracking tables
```

#### daily_agg.py - Daily Aggregation
```python
# Job Definition: hsds-daily-totals-job:2
# Aggregates 3-hourly data into daily summaries
# Uses adjusted dates (previous day) via Lambda function
```

#### catchment_e2e.py - Catchment Visitor/Worker Processing
```python
# Job Definition: hsds-catchment-job:1
# Memory: 16384 MB (16GB) - handles ~10M rows per API call
# vCPUs: 4
# Environment Variables:
# - START_DATE: Target month (YYYY-MM-DD, always 1st of month)
# - END_DATE: Same as START_DATE (single month extraction)
# - CONSUMER_KEY, CONSUMER_SECRET: BT API credentials
#
# Process:
# 1. Pre-flight check: verifies S3 partitions exist (skips if all present)
# 2. Single API call per dataset fetches ~10M rows (all poi_types)
# 3. GroupBy poi_type, writes Hive-partitioned Parquet to S3
#
# Output: s3://hsds-data/bt/catchment/{visitor,worker}/poi_type=X/year=Y/month=Z/data.snappy.parquet
# Date handling: Step Function derives previous month's 1st from weekly startDate via JSONata
#
# Container command:
# ["poetry", "run", "python", "highstreets/aws_pipeline/catchment_e2e.py"]
```

#### catchment_initial_load.py - Catchment Historical Backfill
```python
# One-time script to backfill historical catchment data
# Uses same logic as catchment_e2e.py but iterates over a range of months
# Safe to interrupt and re-run (existence check picks up where it left off)
#
# Usage:
#   python highstreets/aws_pipeline/catchment_initial_load.py --start 2022-05-01 --end 2026-05-01
#   python highstreets/aws_pipeline/catchment_initial_load.py --start 2022-05-01 --end 2026-05-01 --dry-run
```

#### bt_lookups.py - Lookup Processing
```python
# Job Definition: hsds-lookup-job:1
# Updates geographic lookup tables and spatial relationships
```

### Mastercard Processing Modules

#### mcard_weekly.py - Weekly Processing
```python
# Job Definition: hsds-mcard-weekly:1
# Processes weekly Mastercard transaction data
```

#### mcard_weekly_intl.py - International Weekly
```python
# Job Definition: hsds-mcard-intl:1
# Processes international visitor transaction data
```

#### mcard_3hourly.py - 3-Hourly Processing
```python
# Job Definition: hsds-mcard-3hourly:2
# Processes 3-hourly Mastercard transaction data with inflation adjustments
```

## Environment Variables

### Required Environment Variables

All pipeline modules use these core environment variables:

```bash
# Database Configuration
PG_HOST=your-rds-endpoint.eu-west-2.rds.amazonaws.com
PG_DATABASE=highstreets
PG_USER=your_db_user
PG_PASSWORD=your_db_password
PG_PORT=5432

# API Configuration
CONSUMER_KEY=your_bt_consumer_key
CONSUMER_SECRET=your_bt_consumer_secret

# Storage Configuration
BASE_DIR=s3://hsds-data/
AWS_DEFAULT_REGION=eu-west-2

# Processing Configuration (passed via Step Functions)
START_DATE=2023-01-01
END_DATE=2023-01-31
```

### Step Functions Environment Variable Injection

Environment variables are injected via `ContainerOverrides`:

```json
"ContainerOverrides": {
  "Environment": [
    {
      "Name": "START_DATE",
      "Value": "{% $states.input.startDate %}"
    },
    {
      "Name": "END_DATE",
      "Value": "{% $states.input.endDate %}"
    }
  ]
}
```

## Deployment Process

### Manual Deployment Steps

1. **Build and Test Locally**
   ```bash
   docker build -t [service-name]-pipeline .
   docker run --env-file .env [service-name]-pipeline
   ```

2. **ECR Authentication**
   ```bash
   aws ecr get-login-password --region eu-west-2 | docker login --username AWS --password-stdin 590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-[service]
   ```

3. **Build and Tag**
   ```bash
   docker build -t [service-name]-e2e-pipeline .
   docker tag [service-name]-e2e-pipeline:latest 590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-[service]:latest
   ```

4. **Push to ECR**
   ```bash
   docker push 590183914513.dkr.ecr.eu-west-2.amazonaws.com/hsds-[service]:latest
   ```

5. **Update Job Definitions**
   ```bash
   aws batch register-job-definition --cli-input-json file://job-definitions/hsds-[service]-job.json
   ```

6. **Test with Step Functions**
   ```bash
   aws stepfunctions start-execution --state-machine-arn arn:aws:states:eu-west-2:590183914513:stateMachine:hsds-bt-pipeline
   ```

### Automated Deployment

The deployment process can be automated using CI/CD pipelines that follow the same manual steps.

## Monitoring and Logging

### CloudWatch Integration

#### Log Groups
- `/aws/batch/job` - Batch job execution logs
- `/aws/lambda/HSDSProcessDateParameters` - Lambda function logs
- `/aws/stepfunctions/hsds-bt-pipeline` - Step Functions execution logs
- `/aws/stepfunctions/hsds-mcard-pipeline` - Mastercard pipeline logs

#### Monitoring
- **Batch Job Status**: Monitor job success/failure rates
- **Step Functions Execution**: Track workflow completion times
- **Database Performance**: Monitor RDS metrics
- **ECR Repository**: Track image pushes and pulls

### Error Handling

#### Common Issues and Resolution
1. **Batch Job Failures**: 
   - Check CloudWatch logs: `/aws/batch/job`
   - Verify environment variables in job definition
   - Check ECR image availability

2. **Step Functions Failures**:
   - Review execution history in Step Functions console
   - Check Lambda function logs for date processing errors
   - Verify Batch job definitions and queues exist

3. **Database Connection Issues**:
   - Verify RDS security groups allow Batch subnet access
   - Check environment variables: PG_HOST, PG_USER, PG_PASSWORD
   - Validate database credentials and permissions

4. **API Rate Limiting**:
   - Monitor BT API usage patterns
   - Implement exponential backoff in APIClient
   - Check CONSUMER_KEY and CONSUMER_SECRET validity

### Alerting

#### CloudWatch Alarms
- Batch job failure rates > 10%
- Step Functions execution failures
- Long-running jobs (> 2 hours)
- Database connection failures
- ECR push/pull failures

## Security Configuration

### IAM Roles and Policies

#### Batch Execution Role
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::hsds-data",
        "arn:aws:s3:::hsds-data/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "rds-db:connect"
      ],
      "Resource": "arn:aws:rds-db:eu-west-2:590183914513:dbuser:*/highstreets_user"
    }
  ]
}
```

#### Step Functions Role
- `states:StartExecution`
- `batch:SubmitJob`
- `lambda:InvokeFunction`

### Network Security

#### VPC Configuration
- **Region**: eu-west-2
- **Subnets**: Private subnets for Batch compute environment
- **Security Groups**: Allow RDS access on port 5432
- **NAT Gateway**: For internet access from private subnets

## Best Practices

### Performance Optimization
1. **Parallel Processing**: BT pipeline uses parallel execution for independent jobs
2. **Sequential Dependencies**: Mastercard pipeline respects data dependencies
3. **Date Processing**: Lambda functions handle complex date transformations
4. **Resource Right-sizing**: Different job definitions for different workloads

### Cost Optimization
1. **Spot Instances**: Use for non-critical batch workloads
2. **Auto Scaling**: Batch compute environment scales based on demand
3. **Efficient Scheduling**: Run large jobs during off-peak hours
4. **Resource Cleanup**: Automatic cleanup of completed jobs

### Reliability
1. **Retry Logic**: Step Functions implement automatic retries with exponential backoff
2. **Error Handling**: Comprehensive error handling in each pipeline module
3. **Health Checks**: Monitor pipeline health and data quality
4. **Backup Strategies**: Regular RDS backups and S3 versioning

This AWS infrastructure provides a robust, scalable platform for processing London's footfall and transaction data efficiently and reliably. 