---
title: "BT Footfall Data"
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
```
📱 Data Sources        🔄 Processing Pipeline       🗄️ Storage & Distribution
     │                        │                           │
BT API (Weekly)   ────────┐   │   ┌─ Data Validation      │   ┌─ PostgreSQL Database
     │            ┌──────▼───▼───▼─┐                     │   │
Mastercard       │ AWS Processing │ ┌─ Transformation    │   ├─ Data Hub Explorer
Files (Monthly)  │   Pipeline     │ │                   │   │
     │            └──────┬───┬───┬─┘ ├─ Aggregation      │   ├─ London Datastore  
ONS CPI API      ────────┘   │   │   │                   │   │
                             │   └─ Quality Checks      │   └─ Partner Exports
                             │                           │
                         Monitoring                  Distribution
                         & Alerting                  & Analytics
```

### **Processing Architecture**
- **Ingestion Layer**: Automated data collection from multiple sources
- **Processing Layer**: AWS-based transformation and validation pipeline  
- **Storage Layer**: PostgreSQL database with optimized schemas
- **Distribution Layer**: Multiple output channels for different users

## 📱 **BT Footfall Data Workflow**

### **1. Data Collection (Weekly - Mondays)**

#### **Process Overview**
```
🕒 Monday 09:00 → API Call → Data Validation → Initial Processing → Quality Checks
      │              │            │                 │               │
  Schedule         OAuth       Schema Check      Transform     Volume Validation
  Trigger       Authentication                                      │
                                                              ✓ Success / ❌ Alert
```

#### **Detailed Steps**
| Step | Time | Process | Output |
|------|------|---------|--------|
| **1** | 09:00 | **Scheduled API Call** | Trigger automated data collection |
| **2** | 09:05 | **Authentication** | Obtain OAuth token using consumer credentials |
| **3** | 09:10 | **Data Request** | Request previous week's data from BT API |
| **4** | 09:30 | **Data Reception** | Receive JSON response with ~750k records |
| **5** | 09:35 | **Schema Validation** | Validate against BTHexRawSchema |
| **6** | 09:40 | **Initial Quality Check** | Volume and completeness validation |

### **2. Data Transformation (Monday-Tuesday)**

#### **Transformation Pipeline**
```
Raw API Data → Field Mapping → Temporal Processing → Spatial Validation → Database Loading
     │              │              │                    │                 │
 JSON Format   Standard Names   3-Hour Periods    Hex Grid Check    Production Tables
                   │                 │                │                    │
              time_indicator    00-03, 03-06...   8,000 hex cells    bt_footfall_tfl_hex_3hourly
                  to hours        validation         coverage
```

#### **Key Transformations**
| Process | Input | Output | Purpose |
|---------|-------|--------|---------|
| **Field Standardization** | `time_indicator` | `hours` | Consistent column naming |
| **Date Formatting** | Various formats | `count_date (TIMESTAMP)` | Standardized temporal reference |
| **Population Calculation** | Percentages + totals | Resident/visitor/worker counts | Absolute population numbers |
| **Quality Metrics** | Raw percentages | Loyalty %, dwell time | Behavioral insights |

### **3. Geographic Aggregation (Tuesday-Wednesday)**

#### **Boundary Processing**
```
Hex Grid Data → Spatial Joins → Boundary Aggregation → Quality Validation → Output Tables
     │               │               │                    │                │
8k hex cells    Lookup Tables   Sum by boundary      Volume checks    Boundary tables
     │               │               │                    │                │
    BT data     hex_boundary     Aggregate metrics    Trend analysis   Final datasets
               lookup tables      by area/time
```

#### **Aggregation Levels**
| Boundary Type | Process | Output Table | Records/Week |
|---------------|---------|--------------|--------------|
| **High Streets** | Hex → High Street lookup | `econ_busyness_bt_highstreets_3hourly_counts` | ~50,000 |
| **Town Centres** | Hex → Town Centre lookup | `econ_busyness_bt_towncentres_3hourly_counts` | ~40,000 |
| **BIDs** | Hex → BID lookup | `econ_busyness_bt_bids_3hourly_counts` | ~20,000 |
| **Bespoke Areas** | Hex → Bespoke lookup | `econ_busyness_bt_bespokes_3hourly_counts` | ~15,000 |

### **4. Data Distribution (Wednesday)**

#### **Export Generation**
```
Database Tables → Partner Filtering → Data Export → Distribution → Monitoring
     │                │                 │             │            │
Production data   Sublicense rules   CSV files    S3 Storage   Delivery tracking
     │                │                 │             │            │
All boundaries   Geographic filters  Custom formats London      Success/failure
                Area-specific data                  Datastore    alerts
```

## 💳 **Mastercard Transaction Data Workflow**

### **1. File Reception (Monthly - Week 1)**

#### **Process Overview**
```
📧 File Delivery → Validation → Storage → Processing Queue → Notification
     │              │           │           │                │
  SFTP/Email    File integrity S3 bucket   Job scheduling   Team alerts
   transfer      Size, format   secure      Batch jobs      Success/error
                 Hash check     storage     preparation      status
```

#### **File Processing Steps**
| Step | Process | Validation | Output |
|------|---------|------------|--------|
| **1** | **File Reception** | File size, format check | Raw files in staging |
| **2** | **Initial Validation** | Schema, date range check | Validated raw data |
| **3** | **Data Extraction** | Decompress, parse records | Structured dataset |
| **4** | **Quality Assessment** | Volume, completeness check | Quality report |

### **2. Data Processing (Week 1-2)**

#### **Adjustment Pipeline**
```
Raw Data → Geographic Mapping → Adjustment Factors → Inflation Correction → Validation
   │             │                    │                    │               │
Monthly files  Quad→Boundary      Spending Pulse        ONS CPI data   Final dataset
   │             │                adjustment             │               │
CSV format    Spatial joins       Market share         2018 baseline   Quality checks
              London boundaries   Cash-to-card shift   price index     Volume validation
```

#### **Adjustment Process Detail**
| Process | Input | Adjustment Factor | Output | Purpose |
|---------|-------|------------------|--------|---------|
| **Market Share** | Raw transaction amounts | Mastercard market share % | Market-adjusted amounts | Account for total market |
| **Cash-to-Card** | Market-adjusted amounts | Payment method trends | Payment-adjusted amounts | Account for payment shifts |
| **Inflation** | Payment-adjusted amounts | ONS CPI (2018 baseline) | Inflation-adjusted amounts | Enable time comparison |

### **3. Geographic Aggregation (Week 2)**

#### **Boundary Processing**
```
Quad-Level Data → Lookup Joins → Aggregation → Validation → Output Tables
      │              │             │             │            │
   ~25k quads    Spatial mapping  Sum by area   Trend check  Boundary tables
      │              │             │             │            │
 Transaction      Quad→boundary   Area totals   Historical    Production
   records        relationships   by period     comparison     database
```

#### **Aggregation Hierarchy**
```
Raw Quads (25k)
    ├─ High Streets (~200) → econ_busyness_mcard_highstreets_*
    ├─ Town Centres (~170) → econ_busyness_mcard_towncentres_*
    ├─ BIDs (~80) → econ_busyness_mcard_bids_*
    ├─ Bespoke Areas (~50) → econ_busyness_mcard_bespoke_*
    ├─ Boroughs (33) → econ_busyness_mcard_boroughs_*
    ├─ MSOAs (~1000) → econ_busyness_mcard_msoas_*
    └─ Inner/Outer (2) → econ_busyness_mcard_inner_outer_*
```

### **4. Export Generation (Week 3)**

#### **Data Product Creation**
```
Aggregated Data → Format Selection → Quality Check → Export → Distribution
      │               │                │             │          │
  All boundaries   Partner needs    Final validation CSV files  Multiple channels
      │               │                │             │          │
 Multiple time    3-hourly/weekly  Volume/trend     Custom     Partners, public,
  resolutions      based on use     validation      formats    internal teams
```

## 🔍 **Data Quality Workflow**

### **Continuous Monitoring**
```
Data Processing → Quality Checks → Alert System → Investigation → Resolution
      │               │              │             │              │
   Every stage    Automated rules  Email/Slack   Manual review   Process fix
      │               │              │             │              │
  Volume, trend   Threshold-based  Immediate      Data team      System update
   validation     anomaly detect   notification   analysis       Documentation
```

### **Quality Check Types**
| Check Type | Frequency | Threshold | Action |
|------------|-----------|-----------|--------|
| **Volume Validation** | Every processing run | ±20% from expected | Alert + manual review |
| **Trend Analysis** | Daily | Unusual patterns | Investigation |
| **Completeness Check** | Every load | <95% coverage | Processing halt |
| **Data Freshness** | Daily | >48hrs old | Escalation |

## ⚡ **Performance Optimization**

### **AWS Infrastructure**
```
Step Functions → AWS Batch → Docker Containers → RDS Database
      │              │            │                  │
  Workflow        Compute      Processing         Storage
orchestration    environment   containers        Layer
      │              │            │                  │
 Job scheduling   Auto-scaling  Optimized code   Query optimization
 Dependencies     Cost control  Memory tuning    Index management
```

### **Processing Performance**
| Dataset | Processing Time | Optimization Strategy |
|---------|----------------|----------------------|
| **BT Weekly** | 45 minutes | Parallel processing, optimized queries |
| **Mastercard Monthly** | 3 hours | Batch processing, memory optimization |
| **Partner Exports** | 30 minutes | Pre-computed aggregations |
| **Public Datasets** | 15 minutes | Cached results, incremental updates |

## 🚨 **Error Handling & Recovery**

### **Error Response Workflow**
```
Error Detected → Classification → Automated Recovery → Manual Intervention → Resolution
      │              │                │                    │                 │
  System alert   Error type      Retry logic         Team investigation   Process fix
      │              │                │                    │                 │
  Monitoring     Data/System     3x retry           Root cause analysis   Documentation
   system        Network/Logic   Exponential        Impact assessment      Update
                                backoff
```

### **Recovery Procedures**
| Error Type | Automatic Recovery | Manual Steps | Recovery Time |
|------------|-------------------|--------------|---------------|
| **API Timeout** | 3x retry with backoff | Check API status | 5-15 minutes |
| **Data Volume Anomaly** | Processing pause | Investigate source | 30-60 minutes |
| **Database Error** | Transaction rollback | Check DB health | 15-30 minutes |
| **File Corruption** | Request re-delivery | Contact data provider | 2-24 hours |

## 📊 **Workflow Monitoring**

### **Key Performance Indicators**
| Metric | Target | Current Performance | Monitoring Method |
|--------|--------|-------------------|------------------|
| **Data Freshness** | <48 hours | 24 hours average | Automated alerts |
| **Processing Success Rate** | >99% | 99.5% | Daily reports |
| **Data Quality Score** | >95% | 97% average | Quality dashboard |
| **Partner SLA Compliance** | 100% | 98% | Weekly review |

### **Operational Dashboard Metrics**
- **Daily Processing Status**: Success/failure rates by workflow
- **Data Volume Trends**: Historical comparison and anomaly detection  
- **Quality Scores**: Completeness, accuracy, timeliness metrics
- **System Performance**: Processing times, resource utilization
- **Partner Impact**: Export delivery status, data freshness by partner

## 🔗 **Related Information**

- **[BT Footfall Data Flow](03.1-bt-data-flow.md)**: Detailed BT processing steps
- **[Mastercard Transaction Data Flow](03.2-mastercard-data-flow.md)**: Detailed Mastercard processing steps
- **[Data Quality & Validation](03.3-data-quality.md)**: Quality assurance processes
- **[Database Schema](04-database-schema.md)**: Technical table structures
- **[Data Governance](08-data-governance.md)**: Policies and standards

---

**Workflow Summary**:
- **BT Data**: Weekly processing, 45-minute runtime, 99.5% success rate
- **Mastercard Data**: Monthly processing, 3-hour runtime, automated adjustments
- **Quality Assurance**: Continuous monitoring, automated alerts, 97% quality score
- **Performance**: Optimized AWS infrastructure, parallel processing, real-time monitoring

**Next Steps**: Explore detailed [BT Footfall Data Flow](03.1-bt-data-flow.md) 