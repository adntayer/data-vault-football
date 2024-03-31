# Data Model

## Introduction

The Football Data Vault employs the Data Vault modeling approach to organize and manage football-related data effectively. This document provides an overview of the core concepts of the Data Vault model and how they are applied in the context of football data.

## Core Components

**1. Hubs**

**Definition**: Hubs represent the central business entities in the data model and serve as the single source of truth for their respective domains. They store unique instances of business objects and are characterized by their simplicity and stability. Hubs contain hub keys, which are surrogate keys generated by the system to uniquely identify records, along with descriptive attributes that provide additional context about the entity.

**Application**: In the Football Data Vault, hubs are utilized to represent entities such as teams, players, and matches. For example, the "Team Hub" contains records for each football team, including attributes such as team name, country, and formation.

**Example**: The "Team Hub" includes records for popular football clubs like Manchester United, Real Madrid, and Barcelona. Each record contains a unique hub key and descriptive attributes such as team name, country, and formation.

**2. Links**

**Definition**: Links establish relationships between hubs and capture the associations and connections between different entities. They represent the many-to-many relationships that exist between business objects and enable the modeling of complex relationships within the data model. Links contain link keys, which are generated to uniquely identify relationships between hubs, along with any descriptive attributes that characterize the relationship.

**Application**: In football data, links are used to connect teams with players (Team-Player Link) and matches with participating teams (Match-Team Link). For instance, the "Team-Player Link" associates each player with the team(s) they belong to, along with attributes such as the player's position and jersey number.

**Example**: The "Team-Player Link" establishes connections between players and their respective teams. For example, it links Lionel Messi to FC Barcelona with additional attributes such as position (forward) and jersey number (10).

**3. Satellites**

**Definition**: Satellites store additional context and descriptive details about hubs and links. They capture the history of changes to hub and link attributes over time, enabling the tracking of changes and updates to data. Satellites are designed to be flexible and extensible, allowing for the storage of various types of information, including historical versions of data attributes, audit information, and metadata.

**Application**: Satellites in the Football Data Vault contain historical and descriptive information about teams, players, matches, and their respective relationships. This allows for the tracking of changes and updates to data attributes over time.

**Example**: The "Team Satellite" stores historical data such as team rankings, performance statistics, and coaching staff changes over time. For instance, it tracks changes in the ranking of FC Barcelona throughout different seasons along with attributes such as points, goals scored, and managerial changes.

## Core Concepts

**1. Data Vault Architecture**

**Definition**: Business keys and satellite keys are unique identifiers used to identify records within hubs, links, and satellites. Business keys are surrogate keys generated by the system and are used to establish relationships between different entities. Satellite keys, on the other hand, are natural keys derived from the source data and are used to track changes to attributes within satellites. Both keys play a crucial role in ensuring data integrity and consistency within the Data Vault model.

**Application**: The Football Data Vault follows the principles of Data Vault architecture to ensure consistency, scalability, and ease of maintenance. This includes the separation of business keys and descriptive attributes, the use of point-in-time loading, and the integration of data lineage and traceability.

**Example**: In the Football Data Vault, the "Player ID" serves as the business key in the "Player Hub," uniquely identifying each player. Meanwhile, the "Match Date" acts as the satellite key in the "Match Satellite," tracking changes to match attributes over time.

**2. Business Keys and Satellite Keys**

**Definition**: Business keys serve as unique identifiers for records within hubs and links. They are typically surrogate keys generated by the system and are used to establish relationships between different entities. Satellite keys, on the other hand, identify records within satellites and are natural keys derived from the source data. They are often timestamps or sequence numbers that track changes to attributes in satellites.

**Application**: In the Football Data Vault, business keys are used to uniquely identify teams, players, and matches within hubs and links. Satellite keys, on the other hand, are typically timestamps or sequence numbers that track changes to attributes in satellites.

**Example**: The "Player ID" in the "Player Hub" serves as the business key, uniquely identifying each player. Meanwhile, the "Match Date" in the "Match Satellite" acts as the satellite key, tracking changes to match attributes over time.

**3. Point-in-Time Loading**

**Definition**: Point-in-time loading is a technique used to capture historical changes to data over time. It ensures that each version of the data is preserved and can be reconstructed at specific points in time. This enables historical analysis, auditing, and reporting by providing insights into the evolution of data states over time.

**Application**: In the Football Data Vault, point-in-time loading is applied to satellites to track changes to attributes over time. This ensures that historical data versions are retained, facilitating historical analysis of team rosters, player statistics, and match outcomes.

**Example**: The "Player Statistics Satellite" employs point-in-time loading to capture changes in player statistics over different seasons. For example, it tracks the evolution of Lionel Messi's goals and assists per season, enabling historical analysis of his performance.

**4. Data Lineage and Traceability**

**Definition**: Data lineage and traceability refer to the documentation and visualization of the flow of data from its source systems through various transformations and aggregations to its final destination. It provides insights into how data is derived, aggregated, and consumed throughout its lifecycle, enabling transparency and accountability in data processing and analysis.

**Application**: The Football Data Vault incorporates data lineage and traceability features to document the flow of football data from source systems to the Data Vault model. This ensures transparency and accountability in data processing and analysis.

**Example**: The data lineage in the Football Data Vault documents the flow of match data from various sources, such as official football leagues and sports statistics websites, through transformations and aggregations to the final match dataset in the Data Vault. This provides transparency into how match data is sourced, processed, and utilized for analysis.

**5. Scalability and Flexibility**

**Definition**: Scalability and flexibility are key characteristics of the Data Vault model, allowing it to accommodate changes and additions to the data model over time without requiring significant rework. The model is designed to be modular and extensible, enabling the seamless integration of new data sources, entities, and attributes as the project evolves.

**Application**: In the Football Data Vault, scalability and flexibility are achieved through modular design principles and the use of standardized modeling patterns. This allows for the seamless integration of new data sources, entities, and attributes as the project evolves.

**Example**: As the Football Data Vault project evolves, new data sources such as social media sentiment analysis or player performance metrics from wearable devices can be seamlessly integrated into the existing data model. For instance, the addition of social media sentiment data may require the creation of new hubs, links, and satellites to capture and analyze fan sentiment towards teams or players. The modular design of the Data Vault model allows for these changes to be implemented without disrupting existing data structures, ensuring the scalability and flexibility of the Football Data Vault as it grows to accommodate new data sources and analytical requirements.

**6. Data Quality and Consistency**

**Definition**: Data quality and consistency refer to the accuracy, completeness, and reliability of the data stored within the Football Data Vault. It ensures that the data is fit for purpose and meets the required standards for analysis and decision-making.

**Application**: In the Football Data Vault, data quality and consistency are maintained through various measures such as data validation rules, data profiling, and data cleansing processes. These processes help identify and rectify errors, inconsistencies, and duplicates in the data, ensuring its integrity and reliability.

**Example**: Before loading match data into the Football Data Vault, data quality checks are performed to validate the accuracy of match results, player statistics, and other relevant information. Any discrepancies or anomalies are flagged for further investigation and resolution. For instance, if there are inconsistencies in match scores between different sources, data cleansing techniques such as standardization and deduplication are applied to ensure consistency across the dataset.

**7. Error Handling and Exception Management**

**Definition**: Error handling and exception management involve the identification, logging, and resolution of errors and exceptions that occur during data processing and analysis within the Football Data Vault.

**Application**: In the Football Data Vault, error handling and exception management strategies are implemented to ensure the robustness and reliability of data operations. This includes mechanisms for logging errors, notifying stakeholders, and implementing corrective actions to address issues promptly.

**Example**: During the loading of match data into the Football Data Vault, if there is a failure in processing a particular match record due to missing or invalid data, an error is logged, and an exception is raised. The system then triggers a notification to the data steward or administrator, who investigates the issue and takes appropriate action, such as reprocessing the data or updating data validation rules to prevent similar errors in the future.

**8. Data Governance and Compliance**

**Definition**: Data governance and compliance involve the establishment of policies, procedures, and controls to ensure the proper management, protection, and use of data within the Football Data Vault, in accordance with regulatory requirements and organizational standards.

**Application**: In the Football Data Vault, data governance and compliance frameworks are implemented to define roles and responsibilities, enforce data security measures, and monitor data usage to ensure compliance with regulations such as GDPR, CCPA, and industry-specific standards.

**Example**: The Football Data Vault implements role-based access controls to restrict access to sensitive data such as player salaries or injury records to authorized personnel only. Regular audits and compliance assessments are conducted to ensure adherence to data governance policies and regulations, with any violations addressed through corrective actions and remediation measures.

**9. Metadata Management**

**Definition**: Metadata management involves the collection, storage, and management of metadata – descriptive information about the data stored within the Football Data Vault, including its structure, content, and lineage.

**Application**: In the Football Data Vault, metadata management is essential for understanding and interpreting the data stored within the vault. It includes metadata repositories, data dictionaries, and lineage tracking mechanisms to provide insights into the origins, transformations, and usage of data.

**Example**: The Football Data Vault maintains a centralized metadata repository containing information about the structure and content of hubs, links, and satellites, along with data lineage information documenting the flow of data from source systems to the vault. This metadata is used by data analysts and stakeholders to understand the context of the data, track its lineage, and ensure its accuracy and reliability for analysis and decision-making.

**10. Performance Optimization and Querying Strategies**

**Definition**: Performance optimization and querying strategies involve the implementation of techniques and best practices to enhance the performance and efficiency of data retrieval and analysis within the Football Data Vault.

**Application**: In the Football Data Vault, performance optimization strategies include indexing, partitioning, and materialized views to improve query performance and reduce latency. Querying strategies involve the use of optimized SQL queries, data access patterns, and caching mechanisms to retrieve and analyze data efficiently.

**Example**: To improve query performance in the Football Data Vault, indexes are created on commonly queried attributes such as match dates, team names, and player IDs. Additionally, partitioning techniques are applied to distribute data across multiple storage devices or servers, reducing the time required for data retrieval. Querying strategies involve the use of optimized SQL queries that leverage index scans, joins, and aggregations to efficiently retrieve and analyze football-related data for reporting and analysis purposes.

## References

- [Practical Introduction to Data Vault Modeling](https://medium.com/@nuhad.shaabani/practical-introduction-to-data-vault-modeling-1c7fdf5b9014)

- [Data Vault Part 1 - Introduction](https://bitpeak.pl/dv-1/)

- [Data Vault Part 2 - Data modeling](https://bitpeak.pl/dv-2/)

- [Youtube - Understanding Data Vault 2.0](https://www.youtube.com/watch?v=y7faBrUcb74)

- [Youtube - Introduction to Data Vault Modelling with Hans Hultgren](https://www.youtube.com/watch?v=IQUYELrPKlw)

- [Youtube - Data Warehouse Modelling Using Data Vault 2.0 in Fintech Companies | Alumni Talks 2023](https://www.youtube.com/watch?v=qtQ42HxB_Sg)

- [Youtube - MASTERCLASS: Demystifying Data Vault](https://www.youtube.com/watch?v=whGVZF1amI8)

- [Youtube - Data Vault Academy](https://www.youtube.com/@DataVaultAcademy)

Among others