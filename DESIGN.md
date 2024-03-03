# Design Document

By Marcin Borowski

Video overview: <URL HERE>

## Scope

In this section you should answer the following questions:

* What is the purpose of your database?
* Which people, places, things, etc. are you including in the scope of your database?
* Which people, places, things, etc. are *outside* the scope of your database?

## Functional Requirements

In this section you should answer the following questions:

* What should a user be able to do with your database?
* What's beyond the scope of what a user should be able to do with your database?

## Representation

### Entities

In this section you should answer the following questions:

* Which entities will you choose to represent in your database?
* What attributes will those entities have?
* Why did you choose the types you did?
* Why did you choose the constraints you did?

### Relationships

In this section you should include your entity relationship diagram and describe the relationships between the entities in your database.

ER DIAGRAM
```mermaid
---
title: Radio ads
---
erDiagram
    PL_DOW_NAME   ||--|{ DATE_TIME : has
    PL_MONTH_NAME ||--|{ DATE_TIME : has
    MEDIUM        }|--|| BROADCASTER : has
    MEDIUM        }|--|| AD_REACH : has
    TIME_DETAIL   }|--|| DAYPART : is_in
    TIME_DETAIL   }|--|| UNIFIED_LENGTH : has
    BRAND         ||--|{ ADVERTISEMENT : emitted_by
    ADVERTISEMENT }|--|| MEDIUM : emitted_in
    ADVERTISEMENT }|--|| TIME_DETAIL  : have
    DATE_TIME     ||--|{ ADVERTISEMENT : have
    PRODUCT_TYPE  ||--|{ ADVERTISEMENT : is_of

    PRODUCT_TYPE {
    name     product_types
    serial   id
    enum     product_type
    }
    PL_DOW_NAME {
    name     pl_dow_names
    serial   id
    string   name
    }
    PL_MONTH_NAME {
    name     pl_dow_names
    serial   id
    string   name
    }
    DATE_TIME{
    name     date_time
    serial   id
    date     date
    smallint day
    smallint day_of_week
    smallint week   
    smallint year
    smallint month
    }
    MEDIUM {
    name     mediums
    serial   id
    varchar  submedium 
    smallint broadcaster_id
    smallint ad_reach_id      
    }
    BROADCASTER {
    name     broadcasters
    serial   id
    varchar  submedium         
    }
    AD_REACH {
    name     ad_reach
    serial   id
    enum     reach        
    }
    BRAND {
    name     brands
    serial   id
    enum     brand     
    }
    TIME_DETAIL {
    name     ad_time_details
    serial   id
    date     date 
    varchar  ad_slot_hour
    smallint gg
    smallint mm
    smallint length_mod
    smallint daypart_id
    smallint unified_length
    varchar  ad_code
    }
    DAYPART {
    name     dayparts
    serial   id
    enum     daypart
    }
    UNIFIED_LENGTH {
    name     unified_lengths
    serial   id
    enum     length
    }
    ADVERTISEMENT {
    name     ads_desc
    serial   id
    date     date
    varchar  ad_description
    integer  ad_code
    smallint brand_id
    smallint medium_id
    integer  ad_time_details_id
    smallint product_type_id
    integer  cost
    smallint num_of_emissions
    varchar  type
    }
```

## Optimizations

In this section you should answer the following questions:

* Which optimizations (e.g., indexes, views) did you create? Why?

## Limitations

In this section you should answer the following questions:

* What are the limitations of your design?
* What might your database not be able to represent very well?
