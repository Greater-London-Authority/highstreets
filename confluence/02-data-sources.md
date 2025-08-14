---
title: "Data Sources"
space: "CDU"
parent: "Highstreets Data Platform - Overview"
type: "page"
labels: ["data", "sources"]
---
# Data Sources & Coverage

## 📊 **Overview**

The Highstreets Data Platform integrates two primary commercial datasets to provide comprehensive economic insights across London. Each dataset offers unique perspectives on economic activity and complements the other to create a holistic view of London's commercial landscape.

## 📱 **BT Footfall Data**

### **Data Provider**
- **Source**: BT Group (British Telecommunications)
- **Technology**: Mobile network analytics from anonymized mobile phone data
- **Coverage**: London-wide through mobile network infrastructure

### **Data Characteristics**

#### **Spatial Coverage**
- **Geographic Scope**: All 33 London boroughs
- **Spatial Resolution**: Multiple geographic aggregation levels
  - **Hex Grid**: TfL Hex Grid (350-400m grid cells)
  - **MSOA**: Middle Layer Super Output Areas
  - **LSOA**: Lower Layer Super Output Areas 
  - **Administrative**: High Streets, Town Centres, BIDs, Bespoke areas
- **Coordinate System**: EPSG:27700 (British National Grid)

#### **Temporal Coverage**
- **Historical Data**: From May 2022 onwards
- **Update Frequency**: **Weekly** (received every Wednesday for previous week)
- **Time Resolution**: Multiple temporal aggregations
  - **3-hourly intervals**: 8 periods per day (00-03, 03-06, 06-09, 09-12, 12-15, 15-18, 18-21, 21-00)
  - **Hourly intervals**: For MSOA and LSOA data
  - **Daily totals**: Aggregated daily summaries

#### **Population Segments**
| Segment | Description | Use Case |
|---------|-------------|----------|
| **Residents** | People who live in the area | Understanding local population density |
| **Workers** | People who work in the area | Measuring employment activity |
| **Visitors** | People who neither live nor work in the area | Tourism and retail activity |

#### **Behavioral Metrics**
| Metric | Description | Use Case |
|--------|-------------|----------|
| **Dwell Time** | Average time spent in the area | Engagement depth analysis |
| **Loyalty Percentage** | % of visitors who are repeat visitors | Customer retention insights |

### **Data Collection Process**

#### **Multi-Resolution Data Streams**
| Data Stream | Resolution | Output Table |
|-------------|------------|--------------|
| **Hex Grid** | 3-hourly | `bt_footfall_tfl_hex_3hourly` |
| **MSOA** | Hourly | `bt_footfall_msoa_hourly` |
| **LSOA** | Hourly | `bt_footfall_lsoa_hourly` |
| **Daily Totals** | Daily | `econ_busyness_bt_daily_agg_cust_raw` |
| **Outage Monitoring** | Daily | `econ_busyness_bt_outage_data` |

#### **Processing Steps**
1. **Weekly API Calls**: Automated collection every Wednesday across all endpoints
2. **Multi-Resolution Processing**: Parallel collection at hex, MSOA, LSOA, and daily levels
3. **Data Validation**: Schema validation and quality checks across all datasets
4. **Spatial Processing**: Geographic assignment and boundary validation
5. **Temporal Alignment**: Multi-resolution time period processing
6. **Database Loading**: Storage in production tables and aws s3

### **Data Quality Measures**
- **Outage Detection**: Automated identification of data gaps or anomalies across all resolutions
- **Historical Comparison**: Validation against expected patterns
- **Spatial Validation**: Ensuring complete London coverage across all geographic levels
- **Volume Checks**: Monitoring for unusual changes in data volume
- **Cross-Resolution Consistency**: Validation that aggregations align across spatial scales

## 💳 **Mastercard Transaction Data**

### **Data Provider**
- **Source**: Mastercard (through Mastercard Data & Services)
- **Product**: Geo Insights & Spending Pulse data
- **Coverage**: London-wide transaction anonymized and aggregated data

### **Data Characteristics**

#### **Spatial Coverage**
- **Geographic Scope**: London-wide coverage through merchant locations
- **Spatial Resolution**: Quad-level aggregation (variable resolution based on density)
- **Coordinate System**: Latitude/Longitude, converted to EPSG:27700
- **Geographic Matching**: Linked to London administrative boundaries

#### **Temporal Coverage**
- **Historical Data**: From 2018 onwards
- **Update Frequency**: **Monthly** 
- **Time Resolution**: 
  - **3-hourly data**: For detailed temporal analysis
  - **Weekly data**: For trend analysis and comparison

#### **Transaction Categories**
| Category | Description |
|----------|-------------|
| **Total Retail** | All retail transactions |
| **Eating Places** | Food service establishments |
| **Total Apparel** | Clothing and fashion retail |

#### **Transaction Metrics**
| Metric | Description | Use Case |
|--------|-------------|----------|
| **Transaction Amount** | Total spending value | Economic impact measurement |
| **Transaction Count** | Number of transactions | Activity volume tracking |
| **Average Spend** | Average transaction value | Consumer behavior analysis |
| **Account Count** | Number of unique accounts | Customer base measurement |

### **Data Processing Steps**

#### **1. Raw Data Reception**
- **Monthly files** received via secure transfer
- **File validation** and integrity checks
- **Schema validation** against expected format

#### **2. Adjustment Processing**
- **Inflation Adjustment**: Using ONS Consumer Price Index data
- **Market Share Adjustment**: Correcting for Mastercard's market share
- **Cash-to-Card Shift**: Accounting for changing payment behaviors

#### **3. Geographic Processing**
- **Quad-to-Boundary Mapping**: Linking transaction locations to London boundaries
- **Spatial Aggregation**: Rolling up data to different geographic levels
- **Quality Validation**: Ensuring complete geographic coverage

### **Adjustment Methodology**

#### **Spending Pulse Adjustment**
The platform uses Mastercard's Spending Pulse data to adjust raw transaction figures:


-- Simplified adjustment formula
adjusted_amount = raw_amount / adjustment_factor

-- Where adjustment_factor accounts for:
-- 1. Mastercard market share
-- 2. Cash-to-card payment shift
-- 3. Sector-specific trends


#### **Inflation Adjustment**
All monetary values are adjusted to 2018 baseline using ONS CPI data:


-- Inflation adjustment to 2018 baseline
inflation_adjusted_amount = adjusted_amount * (cpi_2018 / cpi_current_period)


## 🗃️ **Supporting Reference Data**

### **ONS Consumer Price Index (CPI)**
- **Source**: Office for National Statistics API
- **Update Frequency**: Monthly
- **Categories**: Overall CPI, Retail CPI, Food & Drink CPI, Clothing CPI
- **Usage**: Inflation adjustment for Mastercard transaction amounts

### **Geographic Boundary Data**
- **High Streets**: Designated high street boundaries
- **Town Centres**: Designated town centre boundaries  
- **Business Improvement Districts (BIDs)**: Designated BID areas
- **Bespoke Areas**: custom-defined areas for specific projects
- **Administrative Boundaries**: Boroughs, MSOAs, LSOAs

### **Lookup Tables**
| Table Purpose | Spatial Relationship |
|---------------|---------------------|
| **Hex-to-High Street** | Links hex cells to high street boundaries |
| **Hex-to-Town Centre** | Links hex cells to town centre boundaries |
| **Hex-to-BID** | Links hex cells to BID boundaries |
| **Hex-to-Bespoke** | Links hex cells to bespoke area boundaries |
| **Quad-to-Boundary** | Links Mastercard quads to all boundary types |
| **MSOA-to-Boundary** | Links MSOAs to administrative boundaries |
| **LSOA-to-Boundary** | Links LSOAs to administrative boundaries |


## 🛡️ **Data Privacy & Compliance**

### **BT Data Privacy**
- **Anonymization**: All data is anonymized at source
- **Aggregation**: Minimum aggregation thresholds applied
- **GDPR Compliance**: Full compliance with data protection regulations
- **Spatial Generalization**: 350m hex grid prevents individual tracking

### **Mastercard Data Privacy**
- **Aggregation**: All data pre-aggregated by Mastercard
- **De-identification**: No individual transaction details
- **Merchant Privacy**: Merchant-specific data not included
- **Geographic Aggregation**: Quad-level spatial aggregation

## 🔍 **Data Quality Indicators**

### **Accuracy Measures**
- **Volume Validation**: Automatic detection of unusual volume changes
- **Trend Analysis**: Comparison with historical patterns
- **Cross-validation**: BT and Mastercard data correlation checks
- **External Validation**: Comparison with other economic indicators

## 📈 **Data Applications**

### **Economic Analysis**
- **Recovery Tracking**: Monitor post-pandemic economic recovery
- **Seasonal Patterns**: Identify peak trading periods and seasonal trends
- **Impact Assessment**: Measure effects of events, policies, or developments
- **Comparative Analysis**: Benchmark different areas and time periods

### **Strategic Planning**
- **Investment Prioritization**: Identify areas for public/private investment
- **Policy Development**: Evidence-based policy making
- **Resource Allocation**: Optimize service delivery based on activity patterns
- **Partnership Development**: Support BID and local authority planning

## 🔗 **Related Information**

- **[Data Processing Workflows](03-data-workflows.md)**: How raw data becomes insights
- **[Database Schema](04-database-schema.md)**: Technical details of data storage
- **[Geographic Boundaries](05-geographic-boundaries.md)**: Spatial framework details
- **[Data Governance](08-data-governance.md)**: Quality standards and compliance

---

**Data Coverage Summary**:
- **BT Coverage**: 100% of London via multi-resolution framework (hex/MSOA/LSOA), 3-hourly/hourly/daily resolution, weekly updates
- **Mastercard Coverage**: London-wide via merchant locations, monthly updates, inflation-adjusted
- **Combined Coverage**: Comprehensive view of both footfall and economic activity across multiple spatial and temporal scales
- **Quality Assurance**: Automated monitoring and validation processes with cross-resolution consistency checks

**Next Steps**: Learn about [Data Processing Workflows](03-data-workflows.md) 