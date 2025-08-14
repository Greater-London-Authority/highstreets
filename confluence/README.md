# Confluence Documentation for Highstreets Data Platform

## 🎯 **Overview**

This directory contains enterprise-grade documentation for the Highstreets Data Platform, designed for automatic synchronization with Confluence using **GitHub Actions + REST API**. The documentation follows a modular, template-based approach that scales with new data sources.

## 🚀 **Automatic Sync System**

### **GitHub Actions Workflow**
Your documentation automatically syncs to Confluence via:
- ✅ **Pull Request Validation**: Dry-run and quality checks on PRs
- ✅ **Automatic Sync**: Push to `aws-hsds` branch triggers Confluence sync
- ✅ **Quality Assurance**: Validates frontmatter, markdown syntax, and links
- ✅ **Error Handling**: Comprehensive error reporting and recovery

### **Workflow Files**
```
.github/
├── workflows/confluence-enterprise.yml    # Main GitHub Actions workflow
└── scripts/
    ├── confluence_sync.py                 # Enterprise sync script
    └── validate_frontmatter.py            # Quality validation
```

## 📋 **Prerequisites**

### **Required GitHub Secrets**
Add these to your repository: **Settings → Secrets → Actions**

```
CONFLUENCE_URL=https://your-company.atlassian.net/wiki
CONFLUENCE_USERNAME=your.email@company.com  
CONFLUENCE_API_TOKEN=your_confluence_api_token
CONFLUENCE_SPACE=CDU
```

### **Confluence API Token**
1. Go to Confluence → Profile → Personal Access Tokens
2. Create a new token with read/write permissions
3. Use this token as `CONFLUENCE_API_TOKEN` (not your password)

## 📊 **Documentation Structure**

### **Core Platform Documentation**
```
📊 Highstreets Data Platform - Overview (01-platform-overview.md)
├── 📱 Data Sources & Coverage (02-data-sources.md)  
├── 🔄 Data Processing Workflows (03-data-workflows.md)
├── 🗃️ Database Schema & Tables (04-database-schema.md)
├── 🤝 Sublicensing & Data Distribution (07-sublicensing.md)
```

### **Detailed Workflows**
```
🔄 BT Footfall Data Flow (03.1-bt-data-flow.md)
🔄 Mastercard Data Flow (03.2-mastercard-data-flow.md)
🏗️ System Architecture (system-architecture.md)
```

## 🎯 **Daily Workflow**

### **Making Documentation Changes**
1. **Edit markdown files** in the `confluence/` directory
2. **Commit and create PR**:
   ```bash
   git checkout -b update/platform-docs
   # Edit documentation
   git add confluence/
   git commit -m "docs: update platform overview"
   git push origin update/platform-docs
   # Create PR → Automatic validation runs
   ```
3. **Review and merge**: GitHub Actions validates and provides feedback
4. **Automatic sync**: Merge to `aws-hsds` triggers Confluence sync

### **Adding New Data Sources**
1. **Copy template**:
   ```bash
   cp confluence/data-source-template.md confluence/tfl-data-source.md
   ```
2. **Update frontmatter**:
   ```yaml
   ---
   title: "TfL Data - Data Source Documentation"
   space: "HSDS"
   parent: "Highstreets Data Platform - Overview"
   type: "page"
   labels: ["tfl", "transport", "data-source", "daily"]
   ---
   ```
3. **Fill in TfL-specific content** following the template structure
4. **Commit and sync**:
   ```bash
   git add confluence/tfl-data-source.md
   git commit -m "feat(docs): add TfL data source documentation"
   git push origin aws-hsds
   # Automatic sync to Confluence
   ```

## 🔧 **File Specifications**

### **Frontmatter Requirements**
Each markdown file must include valid frontmatter:

```yaml
---
title: "Page Title"           # Required: Confluence page title
space: "HSDS"                # Required: Confluence space key  
type: "page"                 # Required: Always "page"
parent: "Parent Page Title"   # Optional: Creates hierarchy
labels: ["tag1", "tag2"]     # Optional: Confluence labels
---
```

### **Supported Markdown Features**
- ✅ **Headers**: All levels (H1-H6)
- ✅ **Tables**: Automatically converted to Confluence tables
- ✅ **Code Blocks**: Language syntax highlighting supported
- ✅ **Lists**: Numbered and bulleted lists
- ✅ **Links**: Internal and external links
- ✅ **Images**: Embedded images and diagrams
- ✅ **Info Boxes**: `**Note:**`, `**Important:**`, `**Success:**` converted to Confluence macros

### **Quality Standards**
The system automatically validates:
- ✅ **Frontmatter completeness**: All required fields present
- ✅ **Title uniqueness**: No duplicate page titles
- ✅ **Parent relationships**: Valid hierarchy references
- ✅ **Markdown syntax**: Proper markdown formatting
- ✅ **Link validation**: Internal and external links work

## 📈 **Monitoring & Status**

### **GitHub Actions Status**
Monitor sync status via:
- **GitHub Actions tab**: Real-time workflow status
- **PR Comments**: Dry-run results and validation feedback
- **Commit Comments**: Sync summaries with success/error counts

### **Confluence Page Hierarchy**
Your documentation creates this structure in Confluence:

```
📊 Highstreets Data Platform - Overview (Root)
├── 📱 BT Footfall Data - Data Source Documentation
│   └── 🔄 BT Footfall Data Flow - Detailed Workflow
├── 💳 Mastercard Transaction Data - Data Source Documentation
│   └── 🔄 Mastercard Transaction Data Flow - Detailed Workflow  
├── 🏗️ System Architecture - Platform Infrastructure
├── 🗃️ Database Schema & Tables
├── 🤝 Sublicensing
```

### **Modular Benefits**
- ✅ **Consistent Structure**: All data sources follow same pattern
- ✅ **Easy Updates**: Change template to update all data sources
- ✅ **Quality Validation**: Template compliance automatically checked
- ✅ **Scalable**: Easy to add new data sources (TfL, ONS, etc.)

## 🔍 **Troubleshooting**

### **Common Issues**

#### **Sync Failures**
If GitHub Actions sync fails:
1. Check **Actions tab** for detailed error logs
2. Verify **Confluence credentials** in repository secrets
3. Ensure **API token permissions** are sufficient
4. Check **frontmatter validation** errors

#### **Page Not Found Errors**
If parent pages aren't found:
1. Ensure **parent page exists** in Confluence
2. Check **parent title spelling** in frontmatter
3. Verify **page hierarchy** is correct
4. Sync **parent pages first** before children

#### **Validation Errors**
If frontmatter validation fails:
1. Check **required fields** (title, space, type)
2. Ensure **no duplicate titles** across files
3. Verify **YAML syntax** is correct
4. Check **label format** (must be list of strings)

### **Getting Help**
1. **Check GitHub Actions logs** for detailed error information
2. **Review PR comments** for validation feedback
3. **Examine workflow files** in `.github/` directory
4. **Contact data team** for Confluence space permissions

## 🎉 **Success Indicators**

Your documentation sync is working when you see:
- ✅ **Green checkmarks** on GitHub Actions workflows
- ✅ **Success comments** on commits with sync summaries
- ✅ **Updated pages** appearing in Confluence automatically
- ✅ **Proper hierarchy** maintained in Confluence space

## 🔗 **Related Resources**

- **[GitHub Actions Documentation](https://docs.github.com/en/actions)**
- **[Confluence REST API](https://developer.atlassian.com/cloud/confluence/rest/)**
- **[Markdown Guide](https://www.markdownguide.org/)**
- **[YAML Frontmatter Syntax](https://yaml.org/)**

---

The GitHub Actions approach provides:
- ✅ **Professional workflow** with PR reviews and validation
- ✅ **Automatic quality checks** preventing broken documentation  
- ✅ **Scalable template system** for future data sources
- ✅ **Enterprise-grade** error handling and monitoring
- ✅ **Team collaboration** through version-controlled documentation

**Next Steps**: Just commit your changes to the `aws-hsds` branch and watch your documentation automatically sync to Confluence!