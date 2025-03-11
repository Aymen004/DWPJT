# Bank Agency Reviews Dashboard Specifications

This document outlines the design and structure for the Looker Studio Dashboard that will visualize insights from our bank reviews data warehouse.

## Dashboard Pages

### 1. Overview Dashboard

**Purpose:** Provide a high-level summary of all reviews across banks and branches.

**Components:**
- Total number of reviews collected (card)
- Average rating across all banks (card)
- Reviews distribution by sentiment (donut chart)
- Top 5 banks by average rating (bar chart)
- Review volume trend over time (line chart)
- Geographic distribution of reviews by city (map)

### 2. Sentiment Analysis Dashboard

**Purpose:** Analyze customer sentiment across different banks and branches.

**Components:**
- Sentiment distribution by bank (stacked bar chart)
- Sentiment trend over time (line chart)
- Top branches with most positive sentiment (table)
- Top branches with most negative sentiment (table)
- Word cloud of positive reviews (word cloud)
- Word cloud of negative reviews (word cloud)

### 3. Topic Analysis Dashboard

**Purpose:** Understand main themes and topics from customer reviews.

**Components:**
- Topic distribution across all reviews (bar chart)
- Topic distribution by bank (heat map)
- Topic by sentiment (stacked bar chart)
- Top topics over time (line chart)
- Topic drill-down table with sample reviews (table)

### 4. Branch Performance Dashboard

**Purpose:** Compare performance across different branches and locations.

**Components:**
- Branch ranking by average rating (sorted table)
- Branch comparison tool (comparison chart)
- Sentiment distribution by city (map)
- Bottom performing branches by sentiment (table with filters)
- Branch improvement over time (sparkline in table)

## Data Connection

Connect this dashboard to the PostgreSQL data warehouse using:

- Host: [DB_HOST]
- Database: bank_reviews_dw
- Schema: marts
- Primary Views/Tables:
  - branch_performance
  - topic_analysis
  - fact_reviews (joined with dimension tables)

## Filters & Controls

Global filters to be implemented:
- Date range selector
- Bank selector
- City/Location selector
- Sentiment filter
- Rating range filter