---
title: "Data Workflow"
space: "CDU"
parent: "Data Sources"
type: "page"
labels: ["bt", "footfall", "data"]
---
# Data Processing Workflows

## 🔄 **Overview**

The Highstreets Data Platform operates two primary data processing workflows that transform raw data from external sources into actionable business insights. Each workflow is optimized for its specific data type and update frequency while maintaining consistent quality standards.

## 🌊 **End-to-End Data Flow**

### **High-Level Process Flow**

📱 Data Sources        🔄 Processing Pipeline       🗄️ Storage & Distribution
     │                        │                           │
BT API (Weekly)   ────────┐   │   ┌─ AWS Step Functions   │   ┌─ PostgreSQL Database
     │            ┌──────▼───▼───▼─┐                     │   │
Mastercard       │ Manual Upload │ ┌─ AWS Batch Jobs     │   ├─ Data Hub Explorer
SFTP (Monthly)   │ + Parameters  │ │                     │   │
     │            └──────┬───┬───┬─┘ ├─ Data Validation   │   ├─ London Datastore  
ONS CPI API      ────────┘   │   │   │                     │   │
                             │   └─ Transformation      │   └─ Partner Exports
                             │       Aggregation        │
                         CloudWatch               S3 Storage
                         Monitoring               & Distribution


### **Processing Architecture**
- **Ingestion Layer**: Manual file uploads (Mastercard) + automated API calls (BT)
- **Orchestration Layer**: AWS Step Functions with parallel/sequential job coordination
- **Processing Layer**: AWS Batch jobs using Docker containers for data transformation
- **Storage Layer**: PostgreSQL database with S3 for file storage and distribution
- **Distribution Layer**: Automated partner exports and public data publishing

## 📱 **BT Footfall Data Workflow**

### **🔧 Manual Step Required**
**Before Processing**: Manually set `startDate` and `endDate` parameters in AWS Step Functions before triggering the workflow.

### **1. AWS Step Functions Orchestration**

#### **Step Function Structure** (Parallel Execution)
```
AWS Step Functions Trigger
├─ Branch 1: Date Processing + Secondary Jobs
│  ├─ Lambda: HSDSProcessDateParameters
│  └─ Parallel Execution:
│     ├─ Outage Batch Job (hsds-bt-outage-job)
│     ├─ MSOA Batch Job (hsds-msoa-job-definition)
│     └─ LSOA Batch Job (hsds-lsoa-job)
├─ Branch 2: Daily Totals
│  ├─ Date Adjustment (subtract 1 day)
│  └─ Daily Totals Batch Job (hsds-daily-totals-job)
├─ Branch 3: Main Processing
│  ├─ Lambda: HSDSProcessDateParameters
│  └─ HEX Batch Job (hsds-bt-hex-job)
└─ Branch 4: Lookup Job (hsds-lookup-job)
```

### **2. Data Processing Pipeline**

#### **Data Collection & Validation**
- **API Source**: BT Business API (OAuth authentication)
- **Data Volume**: ~750,000 records per week (8,000 hex cells × 7 days × 8 time periods)
- **Format**: JSON with footfall percentages and population estimates
- **Validation**: Schema, volume, temporal coverage, spatial coverage checks

#### **Transformation Process**
| Process | Input | Output | AWS Batch Job |
|---------|-------|--------|---------------|
| **Field Mapping** | `time_indicator` → `hours` | Standardized time format | hsds-bt-hex-job |
| **Population Calc** | Percentages + estimates | Absolute counts (resident/visitor/worker) | hsds-bt-hex-job |
| **Quality Checks** | Raw data | Validated dataset | hsds-bt-hex-job |
| **Hex Aggregation** | Hex grid data | Boundary aggregations | hsds-msoa-job, hsds-lsoa-job |

#### **Output Tables**
| Boundary Type | Table Name | AWS Batch Job | Records/Week |
|---------------|------------|---------------|--------------|
| **Hex Grid** | `bt_footfall_tfl_hex_3hourly` | hsds-bt-hex-job | ~750,000 |
| **High Streets** | `econ_busyness_bt_highstreets_3hourly_counts` | hsds-bt-hex-job | ~50,000 |
| **Town Centres** | `econ_busyness_bt_towncentres_3hourly_counts` | hsds-bt-hex-job | ~40,000 |
| **BIDs** | `econ_busyness_bt_bids_3hourly_counts` | hsds-bt-hex-job | ~20,000 |
| **Bespoke Areas** | `econ_busyness_bt_bespokes_3hourly_counts` | hsds-bt-hex-job | ~15,000 |
| **MSOAs** | `bt_footfall_msoa_3hourly` | hsds-msoa-job | ~35,000 |
| **LSOAs** | `bt_footfall_lsoa_3hourly` | hsds-lsoa-job | ~150,000 |

### **3. Distribution & Export**
- **Partner Exports**: Sublicense-specific CSV files uploaded to S3
- **London Datastore**: Public datasets via automated upload
- **Data Hub**: Internal dashboard refresh
- **Quality Reports**: Automated validation summaries

## 💳 **Mastercard Transaction Data Workflow**

### **🔧 Manual Steps Required**
**Before Processing**: 
1. Download files from Mastercard SFTP server
2. Upload to S3 buckets:
   - 3-hourly raw: `s3://hsds-data/mastercard/mrli_3hourly/raw/`
   - Weekly data: `s3://hsds-data/mastercard/weekly/raw/mcard_staging/`
   - Spending Pulse: `s3://hsds-data/mastercard/spendingpulse/received/`

### **1. AWS Step Functions Orchestration**

#### **Step Function Structure** (Sequential Execution)
```
AWS Step Functions Trigger
└─ Sequential Processing:
   ├─ 1. Lookup Job (hsds-lookup-job)
   ├─ 2. Weekly Batch Job (hsds-mcard-weekly)
   ├─ 3. Weekly Intl Batch Job (hsds-mcard-intl)
   └─ 4. Threehourly Batch Job (hsds-mcard-3hourly)
```

### **2. Data Processing Pipeline**

#### **File Processing & Validation**
- **Data Source**: Monthly files via SFTP download (manual upload to S3)
- **Data Volume**: ~2M records per month (~25k quads × multiple time periods)
- **Format**: CSV files with transaction amounts and counts by quad
- **Validation**: File integrity, schema compliance, geographic coverage

#### **Adjustment Process**
| Process | Input | Adjustment Factor | AWS Batch Job | Purpose |
|---------|-------|------------------|---------------|---------|
| **Market Share** | Raw transaction amounts | Mastercard market share % | hsds-mcard-weekly | Account for total market |
| **Cash-to-Card** | Market-adjusted amounts | Payment method trends | hsds-mcard-weekly | Account for payment shifts |
| **Inflation** | Payment-adjusted amounts | ONS CPI (2018 baseline) | hsds-mcard-3hourly | Enable time comparison |

#### **Geographic Aggregation**
| Boundary Type | Table Name | AWS Batch Job | Processing |
|---------------|------------|---------------|------------|
| **Weekly Aggregations** | `econ_busyness_mcard_*_txn` | hsds-mcard-weekly | Weekday/weekend splits |
| **International Data** | `econ_busyness_mcard_*_intl` | hsds-mcard-intl | International card transactions |
| **3-Hourly Data** | `econ_busyness_mcard_*_3hourly_txn` | hsds-mcard-3hourly | Temporal granularity |

#### **Output Tables**
```
Raw Quads (25k) → Geographic Aggregation:
├─ High Streets (~200) → econ_busyness_mcard_highstreets_*
├─ Town Centres (~170) → econ_busyness_mcard_towncentres_*
├─ BIDs (~80) → econ_busyness_mcard_bids_*
├─ Bespoke Areas (~50) → econ_busyness_mcard_bespoke_*
├─ Boroughs (33) → econ_busyness_mcard_boroughs_*
├─ MSOAs (~1000) → econ_busyness_mcard_msoas_*
└─ Inner/Outer (2) → econ_busyness_mcard_inner_outer_*
```

### **3. Distribution & Export**
- **Partner Exports**: Monthly CSV files with geographic and temporal filtering
- **London Datastore**: Anonymized public datasets
- **Data Hub**: Economic indicator dashboards
- **Year-over-Year Analytics**: Growth calculations and trend analysis

## 🔍 **AWS Infrastructure & Monitoring**

### **Step Functions Orchestration**
```
Manual Trigger/Parameters
       ↓
AWS Step Functions
   ├─ Date Processing (Lambda)
   ├─ Job Dependencies (Parallel/Sequential)
   ├─ Error Handling (Retry Logic)
   └─ Status Monitoring
       ↓
AWS Batch Job Queue (hsds-e2e)
   ├─ Docker Container Execution
   ├─ Auto-scaling Compute Environment
   ├─ Job Definition Management
   └─ Resource Optimization
       ↓
PostgreSQL Database + S3 Storage
   ├─ Data Loading & Validation
   ├─ Partner Export Generation
   └─ Quality Reporting
```

### **Quality Monitoring**
| Check Type | Frequency | Monitoring Method | Action |
|------------|-----------|-------------------|--------|
| **Volume Validation** | Every processing run | CloudWatch metrics | Alert + manual review |
| **Processing Success** | Real-time | Step Functions status | Automated retry |
| **Data Quality** | Post-processing | Automated validation | Quality scoring |
| **Partner Delivery** | Weekly/Monthly | S3 upload confirmation | Delivery tracking |

## ⚡ **Performance & Optimization**

### **Processing Performance**
| Workflow | Trigger Frequency | Processing Time | AWS Infrastructure |
|----------|------------------|-----------------|-------------------|
| **BT Footfall** | Weekly (manual trigger) | 45-60 minutes | Parallel Step Functions + Batch |
| **Mastercard Transactions** | Monthly (manual trigger) | 2-3 hours | Sequential Step Functions + Batch |
| **Lookup Updates** | As needed | 15-30 minutes | Shared Batch job |

### **Resource Optimization**
- **Parallel Processing**: BT workflow uses parallel branches for independent jobs
- **Sequential Dependencies**: Mastercard workflow ensures proper data flow order
- **Container Scaling**: AWS Batch auto-scales based on job queue demand
- **Cost Control**: Spot instances and scheduled scaling for cost optimization

## 🚨 **Error Handling & Recovery**

### **Step Functions Error Handling**
```
Job Failure Detection
       ↓
Automatic Retry (3x with exponential backoff)
       ↓
CloudWatch Alert + Team Notification
       ↓
Manual Investigation & Recovery
       ↓
Process Documentation & Improvement
```

### **Common Recovery Scenarios**
| Error Type | Detection Method | Recovery Action | Prevention |
|------------|------------------|-----------------|------------|
| **AWS Batch Job Failure** | Step Functions status | Restart failed job | Improved error handling |
| **Data Volume Anomaly** | Volume validation | Manual investigation | Enhanced monitoring |
| **S3 Upload Failure** | S3 API response | Retry upload operation | Network optimization |
| **Database Connection** | Connection timeout | Database health check | Connection pooling |

## 📊 **Workflow Monitoring Dashboard**

### **Key Performance Indicators**
| Metric | BT Workflow | Mastercard Workflow | Monitoring Method |
|--------|-------------|-------------------|------------------|
| **Success Rate** | 99.5% | 98.8% | Step Functions logs |
| **Processing Time** | 45 min average | 3 hours average | CloudWatch metrics |
| **Data Quality Score** | 97.5% | 96.2% | Automated validation |
| **Partner SLA Compliance** | 99.2% | 98.8% | Delivery tracking |

### **Operational Metrics**
- **AWS Batch Job Status**: Success/failure rates by job definition
- **Resource Utilization**: Compute environment efficiency
- **Cost Analysis**: Processing costs by workflow and time period
- **Data Freshness**: Time from trigger to partner delivery

## 🔗 **Related Information**

- **[BT Footfall Data Flow](03.1-bt-data-flow.md)**: Detailed BT processing steps and AWS job specifics
- **[Mastercard Transaction Data Flow](03.2-mastercard-data-flow.md)**: Detailed Mastercard processing steps and adjustment logic
- **[Database Schema](04-database-schema.md)**: Technical table structures and relationships
- **[Sublicensing & Data Distribution](07-sublicensing.md)**: Partner-specific export configurations
- **[Data Governance](08-data-governance.md)**: Quality standards and compliance framework

---

**Workflow Summary**:
- **BT Data**: Weekly processing via parallel AWS Step Functions, 45-60 minute runtime
- **Mastercard Data**: Monthly processing via sequential AWS Step Functions, 2-3 hour runtime  
- **Manual Coordination**: Parameter setting (BT) and file upload (Mastercard) required
- **AWS Infrastructure**: Step Functions orchestration with Batch job execution and S3 storage
- **High Reliability**: 99%+ success rates with automated monitoring and error recovery

**Next Steps**: Review detailed workflow pages for technical implementation specifics 