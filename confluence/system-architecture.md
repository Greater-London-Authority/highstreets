---
title: "System Architecture - Platform Infrastructure"
space: "HSDS"
parent: "Highstreets Data Platform - Overview"
type: "page"
labels: ["architecture", "aws", "infrastructure", "technical"]
---

# System Architecture - Platform Infrastructure

## 🏗️ **Overview**

The Highstreets Data Platform operates on a modern, cloud-native architecture designed for scalability, reliability, and maintainability. The platform integrates multiple data sources through standardized processing pipelines while maintaining flexibility for source-specific requirements.

## 🏛️ **Architecture Principles**

### **Design Philosophy**
- **Modular Design**: Independent data source processing with shared infrastructure
- **Scalability**: Auto-scaling AWS infrastructure for variable workloads
- **Reliability**: Fault-tolerant design with automated recovery mechanisms
- **Security**: End-to-end encryption and role-based access control
- **Efficiency**: Optimized processing pipelines and resource utilization

### **Technology Stack**
| Layer | Technology | Purpose |
|-------|------------|---------|
| **Orchestration** | AWS Step Functions | Workflow coordination and dependency management |
| **Compute** | AWS Batch, Lambda | Scalable processing and lightweight functions |
| **Storage** | PostgreSQL (RDS), S3 | Structured data and file storage |
| **Integration** | SFTP | External data source connections |
| **Monitoring** | CloudWatch | System health and performance tracking |


### **Multi-Source Data Pipeline Architecture**
*[📋 Interactive Miro Board -  Full Architecture ](https://miro.com/app/board/uXjVKeHa9kQ=/?moveToWidget=3458764636832263342&cot=14)*

**Note:** Visual diagram available above. The interactive board provides complete technical workflow details.

## 🔧 **AWS Infrastructure Components**

### **Compute Infrastructure**
| Service | Purpose | Configuration | Scaling |
|---------|---------|---------------|---------|
| **Step Functions** | Workflow orchestration | State machine definitions | Event-driven |
| **AWS Batch** | Heavy data processing | Docker containers, job queues | Auto-scaling compute |
| **Lambda Functions** | Light processing tasks | Python 3.9 runtime | Concurrent execution |
| **ECS Tasks** | Specialized processing | Custom containers | On-demand scaling |

### **Storage Infrastructure**
| Service | Purpose | Configuration | Backup Strategy |
|---------|---------|---------------|-----------------|
| **RDS PostgreSQL** | Primary data storage | Multi-AZ, 14.x | Automated backups, snapshots |
| **S3 Buckets** | File storage, archives | Multiple buckets by purpose | Versioning, lifecycle policies |

### **Network & Security**
| Component | Purpose | Configuration |
|-----------|---------|---------------|
| **VPC** | Network isolation | Private subnets for processing |
| **Security Groups** | Traffic control | Minimal required access |
| **IAM Roles** | Access management | Least privilege principle |

---

## 🔗 **Related Information**

- **[Platform Overview](01-platform-overview.md)**: Business context and objectives
- **[BT Data Source](bt-data-source.md)**: BT-specific architecture details
- **[Mastercard Data Source](mastercard-data-source.md)**: Mastercard-specific architecture
- **[LDC Premises Data Source](05-ldc-premises-data-source.md)**: LDC premises architecture (Snowflake → PostgreSQL + S3)
- **[Database Schema](04-database-schema.md)**: Data storage architecture
- **[Data Governance](08-data-governance.md)**: Security and compliance architecture

---

**Architecture Summary**:
- **Cloud-Native**: AWS-based infrastructure with auto-scaling capabilities
- **Modular Design**: Standardized patterns with source-specific implementations
- **High Availability**: Multi-AZ deployment with disaster recovery procedures
- **Secure by Design**: Multiple security layers with comprehensive monitoring
- **Future-Proof**: Scalable architecture ready for additional data sources

**Next Steps**: Review source-specific architecture details in [BT Data Source](bt-data-source.md), [Mastercard Data Source](mastercard-data-source.md), and [LDC Premises Data Source](05-ldc-premises-data-source.md) 