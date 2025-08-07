---
title: "Highstreets Data Platform - Overview"
space: "CDU"
type: "page"
labels: ["platform", "overview", "highstreets"]
---

# Highstreets Data Platform - Overview

##  **Platform Purpose**

The Highstreets Data Platform is the Greater London Authority's comprehensive data processing system for London-wide footfall and transaction analysis. It provides critical insights into economic activity across London's high streets, town centres, and business districts.

##  **What We Do**

### **Core Functions**
- **Data Collection**: Automated ingestion from BT footfall API and Mastercard transaction feeds
- **Data Processing**: Transform raw data into actionable insights across multiple geographic boundaries  
- **Data Distribution**: Serve processed data to GLA teams, external partners, and public datasets
- **Analytics Support**: Power the Data Hub explorer and support economic analysis

### **Key Outputs**
- Real-time footfall tracking across London's 350m hex grid
- Transaction volume and spending analysis at multiple geographic levels
- Economic impact assessments for high streets and town centres
- Partner data sharing through sublicense agreements
- Data via London Datastore

##  **Business Objectives**

### **Primary Goals**
1. **Economic Monitoring**: Track recovery and growth across London's commercial areas
2. **Policy Support**: Provide evidence base for economic development decisions
3. **Partnership Value**: Share insights with BIDs, local authorities, and research institutions
4. **Public Transparency**: Make aggregated economic data publicly available

### **Success Metrics**
- **Data Freshness**: BT data updated weekly, Mastercard data updated monthly
- **Geographic Coverage**: 100% of London covered through hex grid and boundary systems
- **Partner Engagement**: 8+ active sublicense agreements serving diverse stakeholders
- **Public Access**: Open datasets available via London Datastore with regular updates

##  **Key Stakeholders**

### **Internal Users**
- **GLA Economics Team**: Primary analysts using the platform for economic research
- **Policy Teams**: Using insights for strategic planning and decision-making
- **Data Science Team**: Platform maintenance and development
- **Communications Team**: Public reporting and data storytelling

### **External Partners**
- **Business Improvement Districts (BIDs)**: Local economic data for their areas
- **Commercial Partners**: Real estate and consultancy firms with data sharing agreements  
- **Academic Researchers**: Universities conducting economic and urban studies
- **Local Authorities**: Borough-level insights for local economic development

### **Data Providers**
- **BT Group**: Mobile network analytics providing footfall insights
- **Mastercard**: Aggregated transaction data (anonymized and GDPR compliant)
- **ONS**: Consumer Price Index data for inflation adjustments

##  **High-Level Architecture**

### **Data Flow Overview**

*[📋 Interactive Miro Board - Complete Platform Data Flow](https://miro.com/app/board/uXjVKeHa9kQ=/?moveToWidget=3458764636705813569&cot=14)*

**Note:** Visual diagram available above. The interactive board provides complete technical workflow details.

### **Core Components**
- **Data Ingestion**: Automated collection from external sources
- **Processing Engine**: AWS-based pipeline for transformation and aggregation
- **Database Layer**: PostgreSQL for structured data storage
- **Distribution Layer**: Multiple output channels for different user needs

##  **Data Coverage**

### **Geographic Scope**
- **Total Coverage**: All 33 London boroughs
- **Spatial Resolution**: 350-400m hex grid cells across Greater London
- **Boundary Types**: High streets, town centres, BIDs, bespoke areas, administrative boundaries

### **Temporal Coverage**
- **Historical Data**: From 2018 onwards (Mastercard), 2022 onwards (BT)
- **Update Frequency**: Weekly (BT), Monthly (Mastercard)
- **Time Resolution**: 3-hourly and weekly intervals for detailed analysis

### **Data Types**
- **Footfall Data**: Resident, visitor, and worker populations by location and time
- **Transaction Data**: Spending volumes, transaction counts, and economic indicators
- **Geographic Data**: Spatial relationships and boundary definitions
- **Adjustment Data**: Inflation adjustments and market share corrections

##  **Use Cases**

### **Economic Analysis**
- **Recovery Tracking**: Monitor post-pandemic economic recovery across London
- **Seasonal Analysis**: Understand patterns in footfall and spending throughout the year
- **Impact Assessment**: Evaluate effects of policy changes, events, or developments
- **Comparative Analysis**: Benchmark performance across different areas and time periods

### **Strategic Planning**
- **Investment Decisions**: Inform public and private investment strategies
- **Policy Development**: Evidence-based policy making for economic development
- **Resource Allocation**: Optimize public resource deployment based on activity patterns
- **Partnership Development**: Support BID and local authority planning initiatives

### **Research & Innovation**
- **Academic Research**: Support university studies on urban economics and behavior
- **Commercial Intelligence**: Provide market insights for real estate and retail sectors
- **Public Reporting**: Regular economic bulletins and public-facing analysis
- **Innovation Projects**: Support pilot programs and experimental initiatives

## 📊 **Platform Benefits**

### **For GLA**
- **Comprehensive View**: Single platform for London-wide economic monitoring
- **Real-time Insights**: Timely data for rapid response and decision-making
- **Cost Efficiency**: Automated processing reduces manual analysis time
- **Quality Assurance**: Standardized data processing ensures consistency

### **For Partners**
- **Localized Data**: Detailed insights specific to partner areas of interest
- **Professional Quality**: Cleaned, validated, and contextually rich datasets
- **Regular Updates**: Automated delivery of fresh data without manual requests
- **Flexible Formats**: Data provided in formats suitable for partner analysis tools

### **For London**
- **Transparency**: Open data supports public understanding of economic trends
- **Innovation**: Platform supports broader ecosystem of data users and innovators
- **Collaboration**: Enables evidence-based collaboration between public and private sectors
- **Economic Development**: Supports informed decisions that benefit London's economy

## 📞 **Getting Started**

### **For GLA Teams**
- Access the [Data Hub Explorer](link-to-data-hub) for interactive analysis
- Review [User Guide](06.2-data-hub.md) for platform navigation
- Contact the data team for advanced analysis requests

### **For External Partners**
- Review [Partner Data Sharing](06.3-partner-sharing.md) for collaboration options
- Explore [Sublicensing Information](07-sublicensing.md) for data sharing agreements
- Check [London Datastore](06.1-london-datastore.md) for public datasets

### **For Researchers**
- Start with [Data Sources & Coverage](02-data-sources.md) to understand available data
- Review [Database Schema](04-database-schema.md) for technical details
- Contact team for research collaboration opportunities

---

**Next Steps**: Explore detailed information about [Data Sources & Coverage](02-data-sources.md) 